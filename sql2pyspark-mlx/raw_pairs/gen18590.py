from pyspark.sql import functions as F

matched = (
    df_orders
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") <= F.date_add(F.col("pr.ends_on"), 7)), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

result = (
    matched
    .groupBy(
        F.when(F.col("promo_id") >= 10, "LARGE").otherwise("SMALL").alias("promo_id_tier"),
        F.date_trunc("week", "order_date").alias("week_start"),
        F.date_trunc("month", "order_date").alias("activity_month"),
    )
    .agg(
        F.max("shipping_fee").alias("max_shipping_fee"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
)
