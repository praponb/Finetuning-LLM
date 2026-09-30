from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "desktop") & F.col("country").isin(["DE", "JP", "TH"]) & ~F.col("country").isin(["DE", "JP", "UK"]))
    .select("id", "browser", "device")
    .orderBy(F.col("device").desc(), "id")
    .limit(50)
)
