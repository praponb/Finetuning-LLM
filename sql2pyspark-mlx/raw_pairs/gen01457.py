from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.date_trunc("YEAR", "event_time").alias("year_start"),
        F.when(F.col("duration_sec") >= 413, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.count_if(F.col("event_type") == "purchase").alias("purchase_events"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.max(F.when(F.col("page") == "/product", F.col("duration_sec"))).alias("product_max_duration_sec"),
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec")).otherwise(0)).alias("add_to_cart_duration_sec"),
    )
    .orderBy(F.col("year_start").desc_nulls_last())
)
