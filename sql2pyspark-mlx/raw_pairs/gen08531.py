from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("rank_in_group").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())
w4 = Window.partitionBy("product_id").orderBy("rank_in_group", "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-08-17")))
    .withColumn("last_restock_label", F.date_format("last_restock", "yyyy-MM-dd"))
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

with_windows = (
    latest
    .filter(F.col("store_id").isin([1, 2, 6]))
    .select(
        "*",
        F.cume_dist().over(w3).alias("quantity_cume_dist"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

result = (
    with_windows
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .cube(F.col("d.city").alias("city"), F.col("d.region").alias("region"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("s.quantity")).alias("min_quantity"),
        F.min(F.col("s.quantity_cume_dist")).alias("min_quantity_cume_dist"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("city").desc_nulls_last())
)
