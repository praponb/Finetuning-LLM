from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("num_attributes_tier").orderBy(F.col("uk_events").desc())

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        F.size("attributes").alias("num_attributes"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.array_contains("coupon_codes", "BUNDLE").alias("used_bundle"),
        F.col("ship_to.country").alias("ship_country"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("num_attributes") >= 72, "HIGH").otherwise("LOW").alias("num_attributes_tier"),
        "ship_country",
    )
    .agg(
        F.size(F.collect_set("ship_country")).alias("num_ship_countries"),
        F.round(F.avg("num_attributes"), 2).alias("avg_num_attributes"),
        F.percentile_approx("num_attributes", 0.95).alias("num_attributes_p95"),
        F.count_if(F.col("ship_country") == "UK").alias("uk_events"),
    )
    .withColumn("uk_events_percent_rank", F.percent_rank().over(w))
)
