from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("page").isin(["/cart", "/search"]))
    .select(
        "id",
        "event_time",
        "session_id",
        F.floor("duration_sec").alias("duration_sec_whole"),
        F.when(F.col("duration_sec") >= 377, "High").when(F.col("duration_sec") >= 236, "Medium").otherwise("Low").alias("duration_sec_band"),
    )
)
