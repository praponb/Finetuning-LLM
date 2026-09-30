from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date").asc_nulls_first(), "id")
w_part = Window.partitionBy("brand").orderBy("cost", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

deduped = (
    df_products
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "category", "brand", "cost", "supplier_id")
)

metrics = (
    deduped
    .select(
        "*",
        F.lead("cost").over(w_part).alias("next_cost"),
        F.lag("cost").over(w_part).alias("prev_cost"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.revenue").alias("store_sale_revenue"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.brand",
        "s.cost",
        "s.next_cost",
        "s.event_seq",
        "s.store_sale_revenue",
        F.col("c.quantity"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "category",
        "next_cost",
        "event_seq",
        "store_sale_revenue",
        "quantity",
    )
    .orderBy(F.col("quantity").desc())
)
