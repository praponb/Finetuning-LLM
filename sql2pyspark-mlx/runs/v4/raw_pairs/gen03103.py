from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.date_trunc("YEAR", "paid_at").alias("year_start"),
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
    )
    .agg(
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.countDistinct("method").alias("distinct_methods"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
    )
    .filter(F.col("distinct_methods") >= 1)
    .orderBy(F.col("quarter_start").desc())
)
