from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("discount").asc_nulls_last())
w_grp = Window.partitionBy("store_id").orderBy("discount", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("discount_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("store_id")
w6 = Window.orderBy("amount_int")
w7 = Window.partitionBy("store_id").orderBy(F.col("amount_int_cume_dist").desc_nulls_last())
w8 = Window.partitionBy("status", "store_id")
w9 = Window.partitionBy("store_id").orderBy(F.col("store_id_sum_discount_percent_rank").asc_nulls_first())

valid = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") == 14))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        F.col("amount").cast("int").alias("amount_int"),
        F.concat("channel", F.lit(" "), "status").alias("channel_status"),
    )
)

scored = (
    valid
    .filter(F.col("shipping_fee").between(19, 22))
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "discount",
        "amount_int",
        F.max("discount").over(w).alias("running_max_discount"),
        F.avg("discount").over(w).alias("running_avg_discount"),
        F.count("discount").over(w).alias("running_count_discount"),
        F.min("discount").over(w).alias("running_min_discount"),
        F.percent_rank().over(w2).alias("discount_percent_rank"),
    )
)

with_windows = (
    scored
    .withColumn("last4_sum_discount_percent_rank", F.sum("discount_percent_rank").over(w_grp))
    .withColumn("last4_max_discount_percent_rank", F.max("discount_percent_rank").over(w_grp))
    .withColumn("last4_avg_discount_percent_rank", F.avg("discount_percent_rank").over(w_grp))
    .withColumn("first_known_running_max_discount", F.first("running_max_discount", ignorenulls=True).over(w4))
)

remaining_keys = (
    with_windows
    .select("store_id", "status")
    .intersectAll(
        df_orders
        .filter(F.col("store_id").isin([27, 30, 40]))
        .select("store_id", "status")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.unit_price") >= 200), "left_anti")
)

with_windows_orders = (
    not_flagged
    .select(
        "store_id",
        "status",
        "amount_int",
        "running_max_discount",
        "last4_max_discount_percent_rank",
        F.sum("discount_percent_rank").over(w_running).alias("store_id_sum_discount_percent_rank"),
        F.min("discount").over(w_running).alias("store_id_min_discount"),
        F.cume_dist().over(w6).alias("amount_int_cume_dist"),
    )
)

scored_orders = (
    with_windows_orders
    .select(
        "amount_int",
        "running_max_discount",
        "store_id_sum_discount_percent_rank",
        "amount_int_cume_dist",
        F.rank().over(w7).alias("amount_int_cume_dist_rank"),
        F.min("amount_int").over(w8).alias("status_min_amount_int"),
        F.cume_dist().over(w9).alias("store_id_sum_discount_percent_rank_cume_dist"),
    )
)

result = (
    scored_orders
    .select(
        "amount_int",
        "running_max_discount",
        "store_id_sum_discount_percent_rank",
        "amount_int_cume_dist",
        "amount_int_cume_dist_rank",
        "status_min_amount_int",
        "store_id_sum_discount_percent_rank_cume_dist",
    )
    .orderBy(
        F.col("amount_int").desc(),
        F.col("store_id_sum_discount_percent_rank").desc(),
    )
)
