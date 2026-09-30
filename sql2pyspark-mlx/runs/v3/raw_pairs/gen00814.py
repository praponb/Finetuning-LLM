from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2024-04-03", "2025-10-23"))
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "customer_id",
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
)
