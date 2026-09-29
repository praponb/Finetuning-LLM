from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("segment", "country").orderBy("age_plus_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment").orderBy("running_avg_shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("running_count_shipping_fee", "id")
w5 = Window.partitionBy("country", "segment").orderBy("running_count_shipping_fee", "id")

valid = (
    df_customers
    .filter(F.col("city").isNotNull() & F.col("city").isin(["Tokyo", "Berlin"]))
    .select(
        "id",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        (F.col("age") + F.col("credit_limit")).alias("age_plus_credit_limit"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount"), F.col("c.shipping_fee"))
)

with_windows = (
    with_latest
    .select(
        "id",
        "email",
        "country",
        "segment",
        "signup_date",
        "shipping_fee",
        F.avg("shipping_fee").over(w_part).alias("running_avg_shipping_fee"),
        F.count("shipping_fee").over(w_part).alias("running_count_shipping_fee"),
        F.sum("shipping_fee").over(w_part).alias("running_sum_shipping_fee"),
    )
)

with_windows_customers = (
    with_windows
    .select(
        "id",
        "country",
        "segment",
        "running_avg_shipping_fee",
        "running_count_shipping_fee",
        F.min("running_avg_shipping_fee").over(w3).alias("running_min_running_avg_shipping_fee"),
        F.max("running_avg_shipping_fee").over(w3).alias("running_max_running_avg_shipping_fee"),
        F.avg("running_avg_shipping_fee").over(w3).alias("running_avg_running_avg_shipping_fee"),
    )
)

metrics = (
    with_windows_customers
    .select(
        "id",
        "running_count_shipping_fee",
        "running_min_running_avg_shipping_fee",
        "running_max_running_avg_shipping_fee",
        "running_avg_running_avg_shipping_fee",
        F.ntile(5).over(w4).alias("running_count_shipping_fee_bucket"),
        F.ntile(3).over(w5).alias("running_count_shipping_fee_bucket_2"),
    )
)

stats = (
    metrics
    .withColumn("running_count_shipping_fee_tier", F.when(F.col("running_count_shipping_fee") >= 13, "HIGH").otherwise("LOW"))
    .withColumn("running_max_running_avg_shipping_fee_tier", F.when(F.col("running_max_running_avg_shipping_fee") >= 30, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["running_count_shipping_fee_tier", "running_max_running_avg_shipping_fee_tier"], ["running_count_shipping_fee_tier"]],
        "running_count_shipping_fee_tier",
        "running_max_running_avg_shipping_fee_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("running_count_shipping_fee_bucket"), 2).alias("avg_running_count_shipping_fee_bucket"),
        F.percentile_approx("running_count_shipping_fee_bucket", [0.25, 0.5, 0.75]).alias("running_count_shipping_fee_bucket_quartiles"),
        F.round(F.avg("running_count_shipping_fee_bucket"), 2).alias("avg_running_count_shipping_fee_bucket_2"),
        F.percentile_approx("running_count_shipping_fee", [0.25, 0.5, 0.75]).alias("running_count_shipping_fee_quartiles"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("distinct_ids") > 1)
)

result = (
    stats
    .select(
        "running_max_running_avg_shipping_fee_tier",
        "distinct_ids",
        "avg_running_count_shipping_fee_bucket",
        "running_count_shipping_fee_bucket_quartiles",
        "avg_running_count_shipping_fee_bucket_2",
        "running_count_shipping_fee_quartiles",
        "gid",
    )
    .orderBy(F.col("gid").desc_nulls_last())
)
