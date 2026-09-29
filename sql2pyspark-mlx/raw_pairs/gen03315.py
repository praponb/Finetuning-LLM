from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .cube(F.col("p.segment").alias("segment"), F.col("p.city").alias("city"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("s.monthly_fee")).alias("avg_monthly_fee"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("segment").asc_nulls_first())
)
