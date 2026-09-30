from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") == "Safari") & F.col("country").isin(["DE", "SG", "US"]))
    .select("id", "browser")
)
