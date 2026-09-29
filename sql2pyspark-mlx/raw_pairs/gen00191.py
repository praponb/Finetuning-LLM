from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isin(["DE", "TH"]))
    .select("country", "device")
    .unionByName(
        df_sessions
        .filter(F.col("device") != "tablet")
        .select("country", "device")
    )
    .distinct()
)
