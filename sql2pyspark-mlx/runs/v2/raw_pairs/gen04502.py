from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "IN_TRANSIT"]))
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("plan").isin(["standard", "basic", "enterprise"]))
        .select("status")
    )
    .orderBy("status")
)
