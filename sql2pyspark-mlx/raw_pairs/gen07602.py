from pyspark.sql import functions as F

matched_keys = (
    df_orders
    .select("status")
    .subtract(
        df_orders
        .filter(F.col("shipping_fee") <= 35)
        .select("status")
    )
)

kept_rows = (
    df_orders
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.date_trunc("month", "order_date").alias("activity_month"),
        F.when(F.col("amount") >= 1710, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
        F.sum(F.when(F.col("channel") == "partner", F.col("discount")).otherwise(0)).alias("partner_discount"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
    )
    .orderBy(F.col("avg_shipping_fee").desc_nulls_first())
)
