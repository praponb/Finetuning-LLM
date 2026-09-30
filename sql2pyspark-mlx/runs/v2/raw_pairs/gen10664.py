from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isNull())
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.col("status").isin(["ACTIVE", "PAUSED"]))
        .select("status")
    )
    .orderBy("status")
)
