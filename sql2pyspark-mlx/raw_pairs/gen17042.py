from pyspark.sql import functions as F

matched_keys = (
    df_orders
    .select("status", "id")
    .intersect(
        df_orders
        .filter(F.col("store_id").isin([21, 26]))
        .select("status", "id")
    )
)

reconciled = (
    df_orders
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("shipping_fee") >= 42, "LARGE").otherwise("SMALL").alias("shipping_fee_tier"),
    )
    .agg(
        F.max("discount").alias("max_discount"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.corr("shipping_fee", "discount").alias("corr_shipping_fee_discount"),
        F.round(F.sum("discount") / F.nullif(F.count(F.when(F.col("status") == "PENDING", 1)), F.lit(0)), 2).alias("discount_per_pending"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("event_count").asc_nulls_last())
)
