from pyspark.sql import functions as F

kept_keys = (
    df_shipments
    .select("order_id", "id")
    .intersectAll(
        df_shipments
        .filter(F.col("delivered_date") > "2025-08-23")
        .select("order_id", "id")
    )
)

verified = (
    df_shipments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .cube(
        F.when(F.col("cost") >= 103, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
        F.date_trunc("quarter", "shipped_date").alias("quarter_start"),
        F.date_trunc("week", "delivered_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("carrier") == "UPS", 1)).alias("ups_rows"),
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
        F.avg(F.when(F.col("carrier") == "Flash", F.col("cost"))).alias("flash_avg_cost"),
    )
    .orderBy(F.col("week_start").desc())
)
