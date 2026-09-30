from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("device").isin(["tablet", "desktop"]) & ~F.col("device").isin(["tablet", "desktop"]))
    .select("id", "browser", "device")
)
