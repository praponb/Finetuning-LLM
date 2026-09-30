from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (F.col("method") != "card"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("status").isin(["FAILED", "REFUNDED"]), "Group A").otherwise("Other").alias("status_group"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("amount") >= 1400, "LARGE").otherwise("SMALL").alias("amount_tier"),
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_rows"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_rows"),
        F.size(F.collect_set("status")).alias("num_status_2"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.avg(F.when(F.col("status") == "FAILED", F.col("amount"))).alias("failed_avg_amount"),
    )
    .filter(F.col("bank_transfer_rows") > 1)
    .orderBy(F.col("num_status_2").desc())
)
