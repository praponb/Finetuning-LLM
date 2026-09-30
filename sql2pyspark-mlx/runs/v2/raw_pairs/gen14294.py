from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/product")
    .select("duration_sec", "page", F.initcap(F.trim("page")).alias("page_title"))
    .orderBy(F.col("duration_sec").desc(), "page")
)
