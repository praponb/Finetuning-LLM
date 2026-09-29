from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("price", "id")
w_grp = Window.partitionBy("product_id").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("discount", "id")
w4 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("discount", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("category", "brand")

with_dims = (
    df_order_items
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.shipping_fee"), F.col("o.discount"), F.col("o.amount"))
)

joined = (
    with_dims
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.product_id",
        "a.unit_price",
        "a.discount",
        "a.amount",
        F.col("d.product_name"),
        F.col("d.category"),
        F.col("d.price"),
    )
)

latest = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    latest
    .filter(F.col("discount") < 0.28)
    .withColumn("first_known_price", F.first("price", ignorenulls=True).over(w_grp))
    .withColumn("next_amount", F.lead("amount", 1).over(w3))
    .withColumn("last_known_discount", F.last("discount", ignorenulls=True).over(w4))
)

enriched = (
    with_windows
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.unit_price",
        "s.discount",
        "s.category",
        "s.next_amount",
        F.col("d.brand"),
        F.col("d.cost"),
        F.col("d.stock_qty"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.avg("cost").over(w5).alias("cost_avg_5_prior"),
        F.last("brand", ignorenulls=True).over(w6).alias("last_known_brand"),
        F.avg("discount").over(w7).alias("category_avg_discount"),
    )
)

result = (
    scored
    .select(
        "id",
        "product_id",
        "unit_price",
        "next_amount",
        "brand",
        "cost_avg_5_prior",
        "last_known_brand",
        "category_avg_discount",
        F.when(F.col("brand").rlike("^(Initech|Umbrella)$"), "GROUP_A").when(F.col("brand").rlike("^(Acme|Globex)$"), "CORE").otherwise("EXTENDED").alias("brand_zone"),
    )
    .orderBy("brand")
)
