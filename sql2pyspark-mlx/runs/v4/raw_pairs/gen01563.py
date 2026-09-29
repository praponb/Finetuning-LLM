from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("duration_sec") > 285)
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.round((F.col("duration_sec") * 0.9), 2).alias("duration_sec_scaled"),
        F.initcap(F.trim("page")).alias("page_title"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("duration_sec_scaled") >= 86, "HIGH").otherwise("LOW").alias("duration_sec_scaled_tier"),
        F.when(F.col("duration_sec") >= 379, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.sum(F.when(F.col("page") == "/cart", F.col("duration_sec_scaled")).otherwise(0)).alias("cart_duration_sec_scaled"),
        F.avg(F.when(F.col("event_type") == "purchase", F.col("duration_sec"))).alias("purchase_avg_duration_sec"),
        F.count_if(F.col("page") == "/product").alias("product_events"),
        F.round(F.sum("duration_sec_scaled") / F.nullif(F.count(F.when(F.col("event_type") == "purchase", 1)), F.lit(0)), 2).alias("duration_sec_scaled_per_purchase"),
    )
    .orderBy("cart_duration_sec_scaled")
)
