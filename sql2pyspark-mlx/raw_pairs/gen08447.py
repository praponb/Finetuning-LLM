from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "customer_id").orderBy("amount", "id")
w_grp = Window.partitionBy("status").orderBy("shipping_fee", "id")
w3 = Window.partitionBy("customer_id").orderBy("order_date_plus_46d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel", "customer_id")
w5 = Window.partitionBy("store_id").orderBy("running_min_shipping_fee", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("channel", "store_id").orderBy("running_sum_shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("shipping_fee").alias("max_order_shipping_fee"))

enriched = (
    df_orders
    .filter(F.col("customer_id").isin([6, 27, 30, 36]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        F.lead("amount", 2).over(w).alias("next_amount"),
        F.ntile(3).over(w_grp).alias("shipping_fee_bucket"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.credit_limit"))
)

valid = (
    with_dims
    .filter((F.col("channel").eqNullSafe(F.lit("mobile"))) & (F.col("shipping_fee_bucket") == 4))
    .withColumn("order_date_plus_46d", F.date_add("order_date", 46))
)

scored = (
    valid
    .select(
        "*",
        F.count("shipping_fee").over(w3).alias("running_count_shipping_fee"),
        F.min("shipping_fee").over(w3).alias("running_min_shipping_fee"),
        F.sum("shipping_fee").over(w3).alias("running_sum_shipping_fee"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "customer_id",
        "order_date",
        "discount",
        "shipping_fee",
        "next_amount",
        "credit_limit",
        "order_date_plus_46d",
        "running_min_shipping_fee",
        F.round((F.col("shipping_fee_bucket") / F.sum("shipping_fee_bucket").over(w4)), 4).alias("shipping_fee_bucket_share"),
        F.avg("running_sum_shipping_fee").over(w5).alias("running_sum_shipping_fee_avg_6_prior"),
        F.last("shipping_fee", ignorenulls=True).over(w6).alias("last_known_shipping_fee"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.delivered_date"), F.col("c.shipped_date"))
)

report = (
    latest_match
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "discount",
        "shipping_fee",
        "next_amount",
        "shipping_fee_bucket_share",
        "running_sum_shipping_fee_avg_6_prior",
        "last_known_shipping_fee",
        F.col("max_order_shipping_fee"),
    )
)

result = (
    report
    .select(
        "id",
        "order_date",
        "discount",
        "shipping_fee",
        "next_amount",
        "shipping_fee_bucket_share",
        "running_sum_shipping_fee_avg_6_prior",
        "last_known_shipping_fee",
        "max_order_shipping_fee",
    )
    .orderBy(F.col("shipping_fee").desc_nulls_first())
)
