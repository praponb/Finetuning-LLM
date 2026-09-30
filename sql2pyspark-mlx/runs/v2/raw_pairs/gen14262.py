from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isin(["basic", "enterprise"]))
    .select("customer_id")
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date").between("2024-03-01", "2024-07-01"))
        .select("customer_id")
    )
    .distinct()
)
