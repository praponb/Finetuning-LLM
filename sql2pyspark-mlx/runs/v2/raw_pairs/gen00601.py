from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") > 410)
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("status").isin(["CANCELLED", "ACTIVE"]))
        .select("status")
    )
    .distinct()
)
