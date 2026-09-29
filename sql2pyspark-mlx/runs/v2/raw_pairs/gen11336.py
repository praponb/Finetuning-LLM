from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isin(["DE", "TH"]))
    .select("id", "browser", "device", "country")
)
