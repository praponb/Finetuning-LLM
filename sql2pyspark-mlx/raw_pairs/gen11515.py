from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .unionByName(
        df_customers
        .filter(F.col("age") == 56)
        .select(F.col("signup_date").alias("start_date"))
    )
    .distinct()
    .orderBy("start_date")
)
