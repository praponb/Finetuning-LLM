from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity_adjusted").desc_nulls_last(), "id")

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-04-05")))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.round((F.col("quantity") * 0.85), 2).alias("quantity_adjusted"),
    )
)

with_dims = (
    filtered
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.sqft"), F.col("s.store_name"))
)

latest = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "product_id", "quantity_adjusted", "sqft")
)

matched_keys = (
    latest
    .select("id", "product_id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock") >= "2023-12-01")
        .select("id", "product_id")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("src.sqft")).alias("avg_sqft"),
        F.avg(F.col("src.quantity_adjusted")).alias("avg_quantity_adjusted"),
    )
    .orderBy(F.col("category").desc_nulls_last())
)
