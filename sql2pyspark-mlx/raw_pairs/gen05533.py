from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc_nulls_last(), "id")
w3 = Window.orderBy(F.col("last_restock").desc(), "id")
w4 = Window.partitionBy(F.date_trunc("week", "last_restock_month_end"), "store_id").orderBy("last_restock", "id")
w5 = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")

cleaned = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-07-17").cast("date")) & F.col("quantity").between(136, 239))
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
)

ranked = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_id", "last_restock")
)

filtered = (
    deduped
    .filter(F.year("last_restock") >= 2023)
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
)

metrics = (
    filtered
    .select(
        "id",
        "store_id",
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("event_seq_2"),
    )
)

filtered_inventory = (
    metrics
    .filter(F.col("row_seq") > 5)
    .select("*", F.floor("event_seq").alias("event_seq_whole"))
)

result = (
    filtered_inventory
    .groupBy("store_id")
    .agg(
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.percentile_approx("row_seq", 0.9).alias("row_seq_p90"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.sum("event_seq_whole").alias("total_event_seq_whole"),
        F.countDistinct("store_id").alias("distinct_store_ids_2"),
        F.avg("row_seq").alias("avg_row_seq"),
    )
    .filter(F.col("total_event_seq_whole") >= 0)
    .orderBy(F.col("total_event_seq_whole").desc())
)
