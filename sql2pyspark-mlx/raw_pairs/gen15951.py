from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter(F.col("amount").between(650, 1600))
    .select(
        "*",
        (F.datediff(F.current_date(), "paid_at") / 30).alias("months_since_paid_at"),
        F.concat("method", F.lit(" "), "status").alias("method_status"),
    )
)

result = (
    filtered
    .cube("method")
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.percentile_approx("months_since_paid_at", [0.25, 0.5, 0.75]).alias("months_since_paid_at_quartiles"),
        F.grouping("method").alias("grouping_id"),
    )
    .orderBy(F.col("method").desc())
)
