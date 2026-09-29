from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["mobile", "desktop", "tablet"]))
    .select("browser", "customer_id")
    .unionByName(
        df_sessions
        .filter(F.col("customer_id") == 32)
        .select("browser", "customer_id")
    )
    .orderBy("customer_id")
)
