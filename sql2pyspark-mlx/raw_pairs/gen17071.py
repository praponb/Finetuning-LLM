from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")
w2 = Window.partitionBy(F.month("opened_date"), "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("region").orderBy(F.col("sqft").desc())
w_running = Window.orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy(F.col("last4_avg_sqft_percent_rank").desc_nulls_last())
w_grp = Window.orderBy("last4_avg_sqft_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("sqft_avg_6", "id").rowsBetween(-2, -1)
w9 = Window.partitionBy(F.month("opened_date")).orderBy("last4_sum_sqft_percent_rank", "id").rowsBetween(-2, Window.currentRow)
w10 = Window.orderBy("last4_avg_sqft_percent_rank", "id")
w11 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

with_windows = (
    df_stores
    .filter(F.col("city").isin(["Tokyo", "Bangkok"]))
    .select(
        "id",
        "opened_date",
        "sqft",
        F.lag("sqft", 1).over(w).alias("prev_sqft"),
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
        F.avg("sqft").over(w3).alias("sqft_avg_6"),
        F.percent_rank().over(w4).alias("sqft_percent_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "opened_date",
        "sqft_avg_6",
        "sqft_percent_rank",
        F.avg("sqft_percent_rank").over(w_running).alias("last4_avg_sqft_percent_rank"),
        F.min("sqft_percent_rank").over(w_running).alias("last4_min_sqft_percent_rank"),
        F.sum("sqft_percent_rank").over(w_running).alias("last4_sum_sqft_percent_rank"),
    )
)

metrics_stores = (
    metrics
    .select(
        "id",
        "opened_date",
        "sqft_avg_6",
        "sqft_percent_rank",
        "last4_min_sqft_percent_rank",
        "last4_sum_sqft_percent_rank",
        F.dense_rank().over(w6).alias("last4_avg_sqft_percent_rank_dense_rank"),
        F.last("sqft_percent_rank", ignorenulls=True).over(w_grp).alias("last_known_sqft_percent_rank"),
        F.avg("sqft_avg_6").over(w8).alias("sqft_avg_6_avg_2_prior"),
        F.avg("sqft_percent_rank").over(w9).alias("sqft_percent_rank_avg_2"),
        F.ntile(10).over(w10).alias("last4_avg_sqft_percent_rank_bucket"),
    )
)

not_flagged = (
    metrics_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date")), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") < F.col("s.opened_date")), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w11))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date").alias("order_order_date"), F.col("c.discount"))
)

report = (
    latest_match
    .select(
        "id",
        "opened_date",
        "sqft_avg_6",
        "last4_min_sqft_percent_rank",
        "last4_sum_sqft_percent_rank",
        "sqft_avg_6_avg_2_prior",
        "sqft_percent_rank_avg_2",
        "last4_avg_sqft_percent_rank_bucket",
        "order_order_date",
    )
    .orderBy(
        F.col("last4_avg_sqft_percent_rank_bucket").asc_nulls_first(),
        F.col("sqft_avg_6").desc(),
    )
)

result = (
    report
    .select(
        "id",
        "opened_date",
        "last4_min_sqft_percent_rank",
        "last4_sum_sqft_percent_rank",
        "sqft_avg_6_avg_2_prior",
        "last4_avg_sqft_percent_rank_bucket",
        "order_order_date",
    )
    .orderBy("sqft_avg_6_avg_2_prior")
)
