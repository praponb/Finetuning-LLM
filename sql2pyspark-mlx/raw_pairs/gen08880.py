from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("channel")
    .agg(
        F.min("discount_pct").alias("min_discount_pct"),
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
    )
)
