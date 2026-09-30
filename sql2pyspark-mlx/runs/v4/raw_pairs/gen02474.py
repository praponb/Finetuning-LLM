from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("order_date") >= F.to_date(F.lit("2023-08-03")))
    .select(
        "id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
    )
)

result = (
    base
    .rollup(
        F.date_trunc("WEEK", "order_date").alias("week_start"),
        F.date_trunc("MONTH", "order_date").alias("activity_month"),
    )
    .agg(
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
        F.stddev_samp("discount").alias("discount_stddev"),
        F.stddev_samp("discount").alias("discount_stddev_2"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_rows"),
        F.percentile_approx("shipping_fee", 0.95).alias("shipping_fee_p95"),
    )
    .filter(F.col("shipping_fee_p95") > 0)
    .orderBy("store_rows")
)
