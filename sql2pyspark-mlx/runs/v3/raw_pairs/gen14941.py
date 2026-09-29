from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select("id", "device")
    .unionByName(
        df_sessions
        .filter(F.col("device").isin(["tablet", "desktop"]))
        .select("id", "device")
    )
    .distinct()
)
