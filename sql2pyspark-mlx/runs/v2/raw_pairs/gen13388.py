from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("status").isin(["ACTIVE", "PAUSED"]))
        .select("status")
    )
    .distinct()
)
