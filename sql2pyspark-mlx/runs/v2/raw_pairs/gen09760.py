from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "ACTIVE"]) & (F.col("start_date") <= "2024-12-01") & F.col("customer_id").isin([10, 11, 17, 30]))
    .select(
        "plan",
        F.when(F.col("plan").isin(["premium", "basic"]), "Core").otherwise("Other").alias("plan_group"),
    )
)
