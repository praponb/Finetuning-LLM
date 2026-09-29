from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 90)
    .select("customer_id")
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date").between("2024-02-24", "2025-10-09"))
        .select("customer_id")
    )
    .distinct()
    .orderBy("customer_id")
)
