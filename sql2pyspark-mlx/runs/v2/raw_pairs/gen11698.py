from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(((F.col("monthly_fee") <= 150) & F.col("end_date").between("2023-06-01", "2024-08-24")) | (F.col("monthly_fee") <= 140))
    .select(
        "id",
        "plan",
        "end_date",
        "status",
        F.when(F.col("plan") == "enterprise", 1).otherwise(0).alias("is_enterprise"),
    )
)
