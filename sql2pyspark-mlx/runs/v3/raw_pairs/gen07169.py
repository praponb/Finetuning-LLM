from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/checkout") & F.col("duration_sec").between(237, 352))
    .select(
        "session_id",
        F.when(F.col("duration_sec") >= 382, "High").when(F.col("duration_sec") >= 97, "Medium").otherwise("Low").alias("duration_sec_band"),
        F.round((F.col("duration_sec") * 1.15), 2).alias("adjusted_duration_sec"),
    )
)
