from pyspark.sql import functions as F

matched_keys = (
    df_orders
    .select("store_id", "channel")
    .exceptAll(
        df_orders
        .filter(F.col("discount") < 0.18)
        .select("store_id", "channel")
    )
)

kept_rows = (
    df_orders
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("year_start", F.date_trunc("YEAR", "order_date"))
    .withColumn("discount_tier", F.when(F.col("discount") >= 0.38, "HIGH").otherwise("LOW"))
    .rollup("year_start", "channel", "discount_tier")
    .agg(
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_rows"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("channel") == "partner", 1)), F.lit(0)), 2).alias("discount_per_partner"),
        F.grouping_id().alias("grouping_id"),
    )
)
