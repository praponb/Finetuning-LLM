from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "value").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("promo").orderBy(F.col("qty").desc())
w3 = Window.partitionBy("promo")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

exploded = (
    df_carts
    .withColumn("rec", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("rec.product_id"),
        F.col("rec.qty"),
        F.col("rec.price"),
        F.col("rec.promo"),
    )
)

lines = (
    exploded
    .select(
        "customer_id",
        "created_at",
        "qty",
        "price",
        "promo",
        F.explode("coupon_codes").alias("value"),
    )
)

with_windows = (
    lines
    .filter(F.col("price") < 200)
    .select(
        "customer_id",
        "qty",
        "value",
        F.count("qty").over(w).alias("qty_count_15m"),
        F.rank().over(w2).alias("qty_rank"),
        F.avg("qty").over(w3).alias("promo_avg_qty"),
    )
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        "s.customer_id",
        "s.value",
        "s.qty_count_15m",
        "s.promo_avg_qty",
        F.col("c.city"),
    )
)

base = (
    with_dims
    .filter(F.col("customer_id").isNotNull())
    .select(
        "customer_id",
        "value",
        "qty_count_15m",
        "promo_avg_qty",
        (F.col("qty_count_15m") * F.col("promo_avg_qty")).alias("qty_count_15m_times_promo_avg_qty"),
        F.round("qty_count_15m", 0).alias("qty_count_15m_rounded"),
    )
)

result = (
    base
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "value",
        "promo_avg_qty",
        F.col("min_subscription_monthly_fee"),
    )
)
