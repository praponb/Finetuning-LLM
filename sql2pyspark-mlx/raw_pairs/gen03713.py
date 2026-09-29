from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_trunc("quarter", "event_time").alias("quarter_start"))
    .agg(
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("event_type") == "click", 1)), F.lit(0)), 2).alias("duration_sec_per_click"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/product", 1)), F.lit(0)), 2).alias("duration_sec_per_product"),
    )
    .filter(F.col("duration_sec_bit_or") > 1)
    .orderBy(F.col("duration_sec_per_click").desc())
)
