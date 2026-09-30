from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["premium", "basic"]) | F.col("start_date").between("2024-09-01", "2025-01-04"))
    .select(
        "id",
        "monthly_fee",
        "end_date",
        F.when(F.col("plan").isin(["basic", "enterprise"]), "Group A").otherwise("Other").alias("plan_group"),
    )
)
