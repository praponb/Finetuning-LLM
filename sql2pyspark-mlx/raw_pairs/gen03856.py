from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").asc_nulls_last(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_first())
w3 = Window.partitionBy("store_id").orderBy("quantity", "id")
w4 = Window.partitionBy(F.month("last_restock"), "store_id").orderBy(F.col("latest_rank").desc(), "id")
w5 = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id")
w6 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_id").orderBy(F.col("latest_rank").desc(), "id")

ranked = (
    df_inventory
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    ranked
    .filter(F.col("latest_rank") < 1)
    .withColumn("quantity_cume_dist", F.cume_dist().over(w2))
    .withColumn("quantity_bucket", F.ntile(3).over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
)

scored = (
    with_windows
    .select(
        "id",
        "product_id",
        "latest_rank",
        "quantity_cume_dist",
        F.row_number().over(w5).alias("event_seq"),
        F.first("row_seq", ignorenulls=True).over(w6).alias("first_known_row_seq"),
    )
)

deduped = (
    scored
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .rollup(
        "product_id",
        F.when(F.col("first_known_row_seq") >= 7, "HIGH").otherwise("LOW").alias("first_known_row_seq_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
        F.percentile_approx("event_seq", 0.9).alias("event_seq_p90"),
    )
    .filter(F.col("row_count") >= 1)
)
