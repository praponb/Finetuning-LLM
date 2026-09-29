from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("start_date") == 2023)
    .select("id", "status")
    .unionByName(
        df_subscriptions
        .filter(F.col("customer_id").isin([12, 25, 36, 39]))
        .select("id", "status")
    )
    .distinct()
)
