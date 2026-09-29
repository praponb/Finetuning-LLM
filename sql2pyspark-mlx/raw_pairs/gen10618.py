from pyspark.sql import functions as F

result = (
    df_promotions
    .rollup("channel", F.date_trunc("WEEK", "starts_on").alias("week_start"))
    .agg(
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
    )
    .orderBy(F.col("channel").desc_nulls_last())
)
