from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")

latest = (
    df_transactions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .groupBy(
        F.date_trunc("QUARTER", "txn_time").alias("quarter_start"),
        F.when(F.col("amount") >= 3600, "HIGH").otherwise("LOW").alias("amount_tier"),
        "merchant",
    )
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
        F.min("txn_time").alias("first_seen"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount"))).alias("credit_total_amount"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
    )
)
