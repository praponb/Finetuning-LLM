from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "channel")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("city")
w5 = Window.partitionBy("channel").orderBy("running_max_discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("running_count_discount").desc(), "id")
w7 = Window.partitionBy("city", "status").orderBy("running_count_discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-06-08"))) & F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        F.initcap(F.trim("channel")).alias("channel_title"),
        F.lower("channel").alias("channel_lower"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "customer_id",
        "status",
        "channel",
        "amount",
        F.min("discount").over(w).alias("running_min_discount"),
        F.avg("discount").over(w).alias("running_avg_discount"),
        F.max("discount").over(w).alias("running_max_discount"),
        F.count("discount").over(w).alias("running_count_discount"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
    )
)

joined = (
    with_windows
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.age"), F.col("p.credit_limit"), F.col("p.city"))
)

with_dims = (
    joined
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.email"),
        F.col("p.age").alias("customer_age"),
        F.col("p.city").alias("customer_city"),
    )
)

not_flagged = (
    with_dims
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.product_id") == 36), "left_anti")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.status",
        "s.channel",
        "s.amount",
        "s.running_min_discount",
        "s.running_avg_discount",
        "s.running_max_discount",
        "s.running_count_discount",
        "s.age",
        "s.credit_limit",
        "s.city",
        "s.email",
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

enriched = (
    with_latest
    .filter(F.col("credit_limit") >= 5000)
    .select(
        "id",
        "status",
        "channel",
        "amount",
        "running_max_discount",
        "running_count_discount",
        "age",
        "credit_limit",
        "city",
        F.stddev_samp("age").over(w4).alias("city_stddev_samp_age"),
        F.last("age", ignorenulls=True).over(w5).alias("last_known_age"),
    )
)

result = (
    enriched
    .select(
        "id",
        "status",
        "channel",
        "running_max_discount",
        "running_count_discount",
        "age",
        "city",
        "city_stddev_samp_age",
        F.ntile(4).over(w6).alias("running_count_discount_bucket"),
        F.sum("running_max_discount").over(w7).alias("running_running_max_discount"),
    )
)
