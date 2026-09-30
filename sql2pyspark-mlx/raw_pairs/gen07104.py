from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("year_start").orderBy(F.col("row_count").desc())

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-09-13 21:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .withColumn("paid_at_date", F.to_date("paid_at"))
    .withColumn("amount_band", F.when(F.col("amount") >= 1530, "High").when(F.col("amount") >= 460, "Medium").otherwise("Low"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("DIRECT")))
)

flagged = (
    cleaned
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        F.date_trunc("year", "paid_at_date").alias("year_start"),
        F.when(F.col("island_id") >= 8, "LARGE").otherwise("SMALL").alias("island_id_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("is_new_burst")).otherwise(0)).alias("bank_transfer_is_new_burst"),
        F.countDistinct("status").alias("distinct_status"),
        F.max("paid_at").alias("last_seen"),
        F.count("*").alias("row_count"),
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst"),
    )
    .withColumn("row_count_dense_rank", F.dense_rank().over(w3))
    .orderBy("year_start")
)
