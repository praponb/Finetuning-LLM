from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") == "TH")
    .select("id", "browser", "device")
    .unionByName(
        df_sessions
        .filter(F.col("browser").isin(["Edge", "Safari"]))
        .select("id", "browser", "device")
    )
    .distinct()
)
