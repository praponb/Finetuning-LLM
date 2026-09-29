from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("start_date", "customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date") > "2025-03-01")
        .select("start_date", "customer_id")
    )
)
