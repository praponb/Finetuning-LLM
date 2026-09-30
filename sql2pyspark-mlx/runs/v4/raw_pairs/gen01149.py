from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("amount").between(1140, 1390))
    .select(
        "*",
        F.coalesce("method", F.lit("unassigned")).alias("method_filled"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("week", "paid_at").alias("week_start"))
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.max("paid_at").alias("last_seen"),
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount")).otherwise(0)).alias("refunded_amount"),
        F.count_if(F.col("status") == "REFUNDED").alias("refunded_events"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("num_methods") < 1)
)
