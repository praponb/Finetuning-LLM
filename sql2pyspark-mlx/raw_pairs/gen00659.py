from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("tag").orderBy(F.col("qty").desc())
w2 = Window.partitionBy("customer_id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

exploded = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select("id", "customer_id", "coupon_codes", F.col("line.qty"))
)

flattened = exploded.select("customer_id", "qty", F.explode_outer("coupon_codes").alias("tag"))

enriched = flattened.select("*", F.cume_dist().over(w_recent).alias("qty_cume_dist"))

with_windows = enriched.withColumn("customer_id_stddev_samp_qty", F.stddev_samp("qty").over(w2))

joined = (
    with_windows
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .filter(~F.col("d.country").isin(["DE", "TH"]))
    .select(
        "a.customer_id",
        "a.qty",
        "a.qty_cume_dist",
        F.col("d.segment"),
        F.col("d.signup_date"),
        F.col("d.country"),
    )
)

enriched_carts = (
    joined
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select(
        "s.customer_id",
        "s.signup_date",
        "s.country",
        F.col("c.credit_limit"),
        F.col("c.age"),
    )
)

result = (
    enriched_carts
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "signup_date",
        "credit_limit",
        "age",
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("age").desc_nulls_first())
)
