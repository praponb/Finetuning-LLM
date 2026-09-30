from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("discount").desc_nulls_first())
w_running = Window.partitionBy("channel").orderBy("amount", "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").desc(), "id")
w_part = Window.partitionBy("customer_id", "segment").orderBy("prev_shipping_fee", "id")

base = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("order_date") >= F.to_date(F.lit("2023-07-03"))))
    .withColumn("years_since_order_date", (F.datediff(F.current_date(), "order_date") / 365))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("DIRECT")))
)

with_windows = (
    base
    .select(
        "id",
        "customer_id",
        "store_id",
        "amount",
        "discount",
        "shipping_fee",
        F.rank().over(w).alias("discount_rank"),
        F.lag("shipping_fee", 1, 0).over(w_running).alias("prev_shipping_fee"),
    )
)

ranked = (
    with_windows
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "customer_id", "discount", "prev_shipping_fee")
)

enriched = (
    ranked
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .select("s.*", F.col("c.city"), F.col("c.segment"), F.col("c.name"))
)

metrics = (
    enriched
    .filter(F.col("prev_shipping_fee") <= 33)
    .select(
        "*",
        F.lead("discount").over(w_part).alias("next_discount"),
        F.lag("discount").over(w_part).alias("prev_discount"),
        F.lag("prev_shipping_fee").over(w_part).alias("prev_prev_shipping_fee"),
        F.lead("prev_shipping_fee").over(w_part).alias("next_prev_shipping_fee"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

stats = (
    metrics
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_rows"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"),
        F.countDistinct("city").alias("distinct_cities"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("prev_discount", [0.25, 0.5, 0.75]).alias("prev_discount_quartiles"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
    )
    .filter(F.col("distinct_ids") < 1000)
)

result = (
    stats
    .select(
        "city",
        "smb_count",
        "distinct_cities",
        "distinct_ids",
        "prev_discount_quartiles",
    )
    .orderBy(F.col("city").asc_nulls_first())
)
