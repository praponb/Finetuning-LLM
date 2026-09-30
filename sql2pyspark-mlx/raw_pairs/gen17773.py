from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "product_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("category").orderBy(F.col("launch_date").desc_nulls_last(), "id")
w3 = Window.partitionBy("category").orderBy(F.col("rank_in_group").desc(), "id")
w4 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id", "category").orderBy(F.col("launch_date").desc(), "id")
w6 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")
w7 = Window.partitionBy(F.year("launch_date"), "category").orderBy(F.col("unit_price").desc(), "id")
w8 = Window.partitionBy(F.date_trunc("MONTH", "launch_date"), "order_id").orderBy("quantity", "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isin([17, 26, 39]))
    .select("*", F.round((F.col("unit_price") * 1.15), 2).alias("unit_price_scaled"))
)

with_dims = (
    filtered
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.category"), F.col("b.cost"), F.col("b.launch_date"))
)

ranked = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

ranked_order_items = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "category",
        "launch_date",
        "rank_in_group",
    )
)

result = (
    ranked_order_items
    .select(
        "id",
        "quantity",
        "unit_price",
        "launch_date",
        "rank_in_group",
        F.first("category", ignorenulls=True).over(w4).alias("first_known_category"),
        F.row_number().over(w5).alias("seq_no"),
        F.lag("quantity", 1).over(w6).alias("prev_quantity"),
        F.ntile(5).over(w7).alias("unit_price_bucket"),
        F.ntile(5).over(w8).alias("quantity_bucket"),
    )
)
