from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .select("device", "customer_id")
    .unionByName(
        df_sessions
        .filter(F.col("device").isin(["mobile", "tablet", "desktop"]))
        .select("device", "customer_id")
    )
)
