from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(~F.col("country").isin(["TH", "SG", "UK"]))
    .select("id", "device", "browser", F.upper("country").alias("country_upper"))
)
