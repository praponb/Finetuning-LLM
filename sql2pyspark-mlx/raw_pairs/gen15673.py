from pyspark.sql import functions as F

dim_totals = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.country").alias("country"), F.col("d.city").alias("city"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("src.monthly_fee")), 2).alias("avg_monthly_fee"),
    )
)

result = (
    dim_totals
    .select(
        "country",
        "city",
        "num_rows",
        F.when(F.col("country").rlike("^(US|DE)$"), "OTHER_ZONE").when(F.col("country").rlike("^(TH|SG)$"), "GROUP_B").otherwise("CORE").alias("country_zone"),
    )
    .orderBy(F.col("city").asc_nulls_first())
)
