from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([17, 32, 34, 37]))
    .select("device", "browser")
    .unionByName(
        df_sessions
        .filter(F.col("device").isin(["mobile", "tablet"]))
        .select("device", "browser")
    )
    .distinct()
    .orderBy("browser")
)
