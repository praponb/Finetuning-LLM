from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Firefox", "Chrome", "Edge"]) & ((F.col("started_at") < "2025-03-01") | F.col("country").isin(["TH", "UK"])))
    .select("id", "started_at", F.upper("country").alias("country_upper"))
    .orderBy("started_at")
)
