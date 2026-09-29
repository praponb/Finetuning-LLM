from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.round((F.datediff("start_date", "end_date") / 30), 1).alias("months_end_date_to_start_date"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_customers.alias("dim"), F.col("s.customer_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["city", "status"], ["city"]],
        F.col("dim.city").alias("city"),
        F.col("s.status").alias("status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.monthly_fee")).alias("min_monthly_fee"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("min_monthly_fee").desc_nulls_first())
)
