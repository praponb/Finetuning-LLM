from pyspark.sql import functions as F

by_dimension = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("dim"), F.col("src.customer_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["segment", "status"], ["segment"], []],
        F.col("dim.segment").alias("segment"),
        F.col("src.status").alias("status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("src.monthly_fee")), 2).alias("avg_monthly_fee"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    by_dimension
    .select("status", "avg_monthly_fee", "grouping_level")
    .orderBy("status")
)
