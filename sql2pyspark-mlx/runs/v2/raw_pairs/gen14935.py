from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        F.when(F.col("duration_sec") >= 370, "High").when(F.col("duration_sec") >= 126, "Medium").otherwise("Low").alias("duration_sec_tier"),
        F.upper("page").alias("page_upper"),
    )
)
