from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.year("start_date") >= 2023)
    .select("start_date", F.lit("subscription").alias("source"))
    .union(
        df_orders
        .filter(F.col("status").isin(["SHIPPED", "PENDING"]))
        .select("order_date", F.lit("order").alias("source"))
    )
)
