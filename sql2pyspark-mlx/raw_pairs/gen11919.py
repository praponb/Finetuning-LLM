from pyspark.sql import functions as F

matched_keys = (
    df_orders
    .select("id")
    .intersectAll(
        df_orders
        .filter(F.col("store_id").isNotNull())
        .select("id")
    )
)

verified = (
    df_orders
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("shipping_fee") >= 41, "HIGH").otherwise("LOW").alias("shipping_fee_tier"),
        "store_id",
        "customer_id",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.corr("shipping_fee", "amount").alias("corr_shipping_fee_amount"),
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
    )
    .filter(F.col("amount_p90") > 0)
    .orderBy(F.col("corr_shipping_fee_amount").asc_nulls_last())
)
