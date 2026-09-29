from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .select("id", "browser", "device")
    .unionByName(
        df_sessions
        .filter(F.col("device") == "tablet")
        .select("id", "browser", "device")
    )
    .distinct()
    .orderBy("browser")
)
