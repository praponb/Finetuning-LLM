from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("txn_type").orderBy("amount", "id")

metrics = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "*",
        F.lag("amount").over(w_recent).alias("prev_amount"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("amount") >= 3700, "HIGH").otherwise("LOW").alias("amount_tier"),
        F.when(F.col("prev_amount") >= 3200, "PREMIUM").otherwise("STANDARD").alias("prev_amount_tier"),
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
    )
    .agg(
        F.count_if(F.col("txn_type") == "CREDIT").alias("credit_events"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.sum("row_seq").alias("total_row_seq"),
    )
    .filter(F.col("credit_events") >= 1)
    .orderBy(F.col("amount_tier").desc())
)
