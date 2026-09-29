from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/search", "/home", "/product"]) & (F.col("duration_sec") >= 430))
    .select("id", "duration_sec")
)
