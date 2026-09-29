from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        F.round((F.col("discount") / F.col("amount")), 2).alias("discount_per_amount"),
        F.lower("channel").alias("channel_lower"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

result = (
    valid
    .groupBy(F.date_trunc("YEAR", "order_date").alias("year_start"), "channel")
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.round(F.avg("discount_per_amount"), 2).alias("avg_discount_per_amount"),
        F.percentile_approx("discount_per_amount", [0.25, 0.5, 0.75]).alias("discount_per_amount_quartiles"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.corr("discount", "shipping_fee").alias("corr_discount_shipping_fee"),
    )
    .orderBy("channel")
)
