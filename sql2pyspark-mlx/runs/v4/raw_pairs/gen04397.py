from pyspark.sql import functions as F

with_window = (
    df_orders
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & F.col("a.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    with_window
    .groupBy(F.date_trunc("quarter", "order_date").alias("quarter_start"), "status")
    .agg(
        F.round(F.sum("promo_id") / F.nullif(F.count(F.when(F.col("status") == "RETURNED", 1)), F.lit(0)), 2).alias("promo_id_per_returned"),
        F.percentile_approx("shipping_fee", 0.5).alias("shipping_fee_p50"),
        F.max("promo_id").alias("max_promo_id"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_rows"),
    )
    .orderBy(F.col("status").desc())
)
