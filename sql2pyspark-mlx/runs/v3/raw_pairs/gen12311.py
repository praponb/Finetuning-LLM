from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country") != "DE")
    .select("browser", "country")
    .distinct()
)
