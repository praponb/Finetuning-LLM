from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["city", "status"], ["city"], []],
        F.col("c.city").alias("city"),
        F.col("s.status").alias("status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("s.balance")), 2).alias("avg_balance"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy("avg_balance")
)
