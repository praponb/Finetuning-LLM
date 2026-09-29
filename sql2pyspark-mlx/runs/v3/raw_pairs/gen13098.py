from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .select("id", "status", "end_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("start_date") > "2024-10-30")
        .select("id", "status", "end_date")
    )
    .orderBy("end_date")
)
