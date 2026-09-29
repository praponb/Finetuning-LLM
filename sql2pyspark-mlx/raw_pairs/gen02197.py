from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("status") == "CANCELLED")
        .select("status")
    )
    .orderBy("status")
)
