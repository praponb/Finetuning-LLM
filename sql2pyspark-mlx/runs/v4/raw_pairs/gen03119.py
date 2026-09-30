from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id", "category").orderBy(F.col("last_restock").desc_nulls_last(), "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2024-02-01").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

filtered = (
    valid
    .filter(F.col("store_id").isin([2, 3, 4, 5]))
    .withColumn("adjusted_quantity", F.round((F.col("quantity") * 0.85), 2))
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_dims = (
    deduped
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.brand"), F.col("d.price"), F.col("d.category"))
)

deduped_inventory = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped_inventory
    .groupBy(F.date_trunc("YEAR", "last_restock_month_end").alias("year_start"))
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("category").alias("distinct_categories"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
)
