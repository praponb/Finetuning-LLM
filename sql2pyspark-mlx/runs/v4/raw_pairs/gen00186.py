from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("monthly_fee").desc(), "id", "key", "subscription_id")
w2 = Window.partitionBy("customer_id", "plan").orderBy("monthly_fee", "id", "key", "subscription_id").rowsBetween(-5, -1)
w3 = Window.partitionBy("plan", "key").orderBy(F.col("subscription_id").desc(), "id", "key", "subscription_id")
w4 = Window.partitionBy("plan")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("key", "v"),
    )
)

with_dims = (
    flattened
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.email"), F.col("c.signup_date"))
)

with_window = (
    with_dims
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.signup_date") >= F.col("sub.start_date")) & (F.col("e.signup_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

deduped = (
    with_window
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    deduped
    .filter(F.col("key").isNotNull())
    .withColumn("subscription_id_times_dedup_rank", (F.col("subscription_id") * F.col("dedup_rank")))
)

metrics = (
    filtered
    .select(
        "*",
        F.avg("subscription_id").over(w2).alias("subscription_id_avg_5_prior"),
        F.ntile(10).over(w3).alias("subscription_id_bucket"),
        F.count("monthly_fee").over(w4).alias("plan_count_monthly_fee"),
    )
)

result = (
    metrics
    .groupBy("key")
    .pivot("plan", ["standard", "enterprise", "basic"])
    .agg(F.sum("plan_count_monthly_fee").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
