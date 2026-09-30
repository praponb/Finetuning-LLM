from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "channel").orderBy("discount", "id")
w_hist = Window.partitionBy("status").orderBy(F.col("discount").desc_nulls_first())
w_part = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")
w_grp = Window.partitionBy(F.month("order_date"), "customer_id").orderBy("order_date", "id").rowsBetween(-6, -1)
w_running = Window.partitionBy("customer_id").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w_recent = Window.partitionBy("customer_id").orderBy("next_discount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city").orderBy(F.col("chiang_mai_rows").desc())

base = (
    df_orders
    .filter(F.col("channel").eqNullSafe(F.lit("store")))
    .withColumn("order_date_minus_45d", F.date_sub("order_date", 45))
    .withColumn("shipping_fee_plus_discount", (F.col("shipping_fee") + F.col("discount")))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("OTHER")))
)

kept = (
    base
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.status") == "LOST"), "left_anti")
)

enriched = (
    kept
    .select(
        "*",
        F.ntile(3).over(w).alias("discount_bucket"),
        F.cume_dist().over(w_hist).alias("discount_cume_dist"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "customer_id",
        "amount",
        "discount",
        "order_date_minus_45d",
        "discount_bucket",
        "discount_cume_dist",
        F.lead("discount_bucket").over(w_part).alias("next_discount_bucket"),
        F.lag("amount").over(w_part).alias("prev_amount"),
        F.row_number().over(w_part).alias("row_seq"),
        F.avg("discount").over(w_grp).alias("discount_avg_6_prior"),
    )
)

scored = (
    with_windows
    .filter(F.col("customer_id").isin([6, 13, 24]))
    .select(
        "*",
        F.max("discount_cume_dist").over(w_running).alias("last7_max_discount_cume_dist"),
        F.sum("discount_cume_dist").over(w_running).alias("last7_sum_discount_cume_dist"),
        F.min("discount_cume_dist").over(w_running).alias("last7_min_discount_cume_dist"),
        F.count("discount_cume_dist").over(w_running).alias("last7_count_discount_cume_dist"),
        F.sum("discount_avg_6_prior").over(w_recent).alias("running_discount_avg_6_prior"),
    )
)

with_dims = (
    scored
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.discount",
        "s.discount_bucket",
        "s.discount_cume_dist",
        "s.prev_amount",
        "s.last7_max_discount_cume_dist",
        "s.last7_sum_discount_cume_dist",
        F.col("p.city"),
        F.col("p.email"),
        F.col("p.credit_limit"),
    )
)

joined = (
    with_dims
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.discount",
        "a.discount_bucket",
        "a.last7_max_discount_cume_dist",
        "a.last7_sum_discount_cume_dist",
        "a.city",
        "a.email",
        "a.credit_limit",
        F.col("p.segment"),
        F.col("p.age"),
    )
)

result = (
    joined
    .groupBy(
        "city",
        F.when(F.col("discount") >= 0.44, "HIGH").otherwise("LOW").alias("discount_tier"),
    )
    .agg(
        F.avg("last7_max_discount_cume_dist").alias("avg_last7_max_discount_cume_dist"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.max(F.when(F.col("city") == "Bangkok", F.col("last7_max_discount_cume_dist"))).alias("bangkok_max_last7_max_discount_cume_dist"),
        F.sort_array(F.collect_set("city")).alias("city_list_2"),
    )
    .withColumn("chiang_mai_rows_dense_rank", F.dense_rank().over(w7))
)
