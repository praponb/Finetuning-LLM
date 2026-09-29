from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("event_count_2").desc())

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    unpacked
    .groupBy(
        "plan",
        F.when(F.col("avg_tier_price") >= 26, "PREMIUM").otherwise("STANDARD").alias("avg_tier_price_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.percentile_approx("entry_price", 0.9).alias("entry_price_p90"),
        F.avg(F.when(F.col("plan") == "starter", F.col("entry_price"))).alias("starter_avg_entry_price"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_rows"),
    )
    .withColumn("event_count_2_dense_rank", F.dense_rank().over(w))
)
