from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page").isin(["/home", "/checkout"]) & F.col("page").isin(["/product", "/home"])) | (F.col("page") != "/home"))
    .select("session_id", "page")
)
