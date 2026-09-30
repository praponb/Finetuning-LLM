from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("started_at") >= "2023-05-01") & (F.col("browser") == "Firefox"))
    .select("country", "browser")
    .orderBy(F.col("country").desc())
)
