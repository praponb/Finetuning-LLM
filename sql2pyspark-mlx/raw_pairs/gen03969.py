from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 424, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.sum(F.when(F.col("page") == "/cart", F.col("duration_sec")).otherwise(0)).alias("cart_duration_sec"),
        F.sum(F.when(F.col("event_type") == "click", F.col("duration_sec")).otherwise(0)).alias("click_duration_sec"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
    )
    .filter(F.col("cart_duration_sec") > 0)
    .orderBy(F.col("cart_duration_sec").desc())
)
