from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "ACTIVE"]))
    .select("plan", "customer_id")
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date") > "2024-08-01")
        .select("plan", "customer_id")
    )
    .distinct()
    .orderBy("customer_id")
)
