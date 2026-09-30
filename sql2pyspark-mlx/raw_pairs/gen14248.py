from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.inline("items"))

result = (
    lines
    .groupBy(F.when(F.col("qty") >= 5, "HIGH").otherwise("LOW").alias("qty_tier"))
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
        F.percentile_approx("qty", [0.25, 0.5, 0.75]).alias("qty_quartiles"),
        F.count_if(F.col("promo") == "WELCOME5").alias("welcome5_events"),
        F.percentile_approx("price", 0.95).alias("price_p95"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
    )
    .filter(F.col("price_p95") >= 0)
)
