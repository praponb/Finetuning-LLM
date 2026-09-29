from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/product")
    .select("duration_sec")
)
