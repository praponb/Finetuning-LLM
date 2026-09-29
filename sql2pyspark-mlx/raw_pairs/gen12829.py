from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy("status", "order_id").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("is_bank_transfer").desc(), "id")
w4 = Window.orderBy("is_bank_transfer_bucket", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.orderBy(F.col("cumulative_is_bank_transfer").desc())

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-10-05 16:00:00"))) & (F.col("order_id") == 12))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("method") == "bank_transfer", 1).otherwise(0).alias("is_bank_transfer"),
    )
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    latest
    .select(
        "id",
        "amount",
        "paid_at",
        "is_bank_transfer",
        F.sum("is_bank_transfer").over(w2).alias("cumulative_is_bank_transfer"),
        F.ntile(4).over(w3).alias("is_bank_transfer_bucket"),
    )
)

scored = (
    enriched
    .filter(F.col("amount") <= 640)
    .select(
        "id",
        "cumulative_is_bank_transfer",
        "is_bank_transfer_bucket",
        F.avg("cumulative_is_bank_transfer").over(w4).alias("cumulative_is_bank_transfer_avg_6"),
        F.cume_dist().over(w5).alias("cumulative_is_bank_transfer_cume_dist"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("cumulative_is_bank_transfer_cume_dist") >= 0.81, "PREMIUM").otherwise("STANDARD").alias("cumulative_is_bank_transfer_cume_dist_tier"),
    )
    .agg(
        F.sum("is_bank_transfer_bucket").alias("total_is_bank_transfer_bucket"),
        F.round(F.avg("cumulative_is_bank_transfer"), 2).alias("avg_cumulative_is_bank_transfer"),
        F.count("*").alias("row_count"),
        F.percentile_approx("cumulative_is_bank_transfer_cume_dist", 0.9).alias("cumulative_is_bank_transfer_cume_dist_p90"),
    )
)
