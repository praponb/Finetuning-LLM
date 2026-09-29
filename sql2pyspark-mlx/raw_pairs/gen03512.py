from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipping_fee").asc_nulls_last())
w2 = Window.partitionBy("customer_id").orderBy(F.col("discount").desc())
w3 = Window.orderBy("discount", "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("discount").desc(), "id")
w_running = Window.partitionBy("customer_id", "status").orderBy("discount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status", "store_id")
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("discount").alias("min_order_discount"))

valid = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("web"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "discount",
        "shipping_fee",
        F.concat("channel", F.lit(" "), "status").alias("channel_status"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

metrics = (
    valid
    .withColumn("shipping_fee_cume_dist", F.cume_dist().over(w))
    .withColumn("discount_percent_rank", F.percent_rank().over(w2))
    .withColumn("discount_bucket", F.ntile(4).over(w3))
    .withColumn("discount_bucket_2", F.ntile(4).over(w4))
)

joined = (
    metrics
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select("s.*", F.col("d.age"), F.col("d.email"))
)

metrics_orders = (
    joined
    .filter(F.col("status") == "SHIPPED")
    .select(
        "id",
        "customer_id",
        "status",
        "shipping_fee",
        "shipping_fee_cume_dist",
        "discount_percent_rank",
        "age",
        "email",
        F.avg("age").over(w_running).alias("running_avg_age"),
        F.min("age").over(w_running).alias("running_min_age"),
        F.sum("age").over(w_running).alias("running_sum_age"),
        F.round((F.col("discount_bucket_2") / F.sum("discount_bucket_2").over(w6)), 4).alias("discount_bucket_2_share"),
    )
)

with_dims = (
    metrics_orders
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.city"), F.col("d.country"), F.col("d.segment"))
)

kept = (
    with_dims
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.amount") > 540), "left_anti")
)

result = (
    kept
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        "discount_percent_rank",
        "age",
        "email",
        "running_avg_age",
        "running_min_age",
        "running_sum_age",
        F.col("min_order_discount"),
    )
    .orderBy(F.col("customer_id").desc(), "shipping_fee")
)
