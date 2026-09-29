from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(F.col("revenue").alias("value"))
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date") >= "2024-04-01")
        .select(F.col("monthly_fee").alias("value"))
    )
    .distinct()
    .orderBy("value")
)
