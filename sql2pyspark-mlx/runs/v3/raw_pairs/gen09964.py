from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.when(F.col("c.country").isin(["JP", "UK"]), "tier_1").otherwise("other").alias("country_group"),
        F.col("c.is_active"),
    )
    .agg(F.avg(F.col("a.balance")).alias("avg_balance"))
    .orderBy("country_group")
)
