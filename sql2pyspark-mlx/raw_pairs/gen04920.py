from pyspark.sql import functions as F

rollup_totals = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .rollup(F.col("c.country").alias("country"), F.col("a.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("a.monthly_fee")), 2).alias("total_monthly_fee"),
    )
)

result = (
    rollup_totals
    .select(
        "country",
        "num_rows",
        F.when(F.col("country").rlike("^(US|DE)$"), "GROUP_A").otherwise("PRIORITY").alias("country_zone"),
    )
    .orderBy("num_rows")
)
