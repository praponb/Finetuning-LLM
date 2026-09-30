from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "store_id").orderBy(F.col("discount").desc_nulls_first())
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("month", "order_date"), "channel")
w4 = Window.partitionBy("customer_id").orderBy("order_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("discount_dense_rank", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.date_trunc("MONTH", "order_date"), "plan").orderBy("amount", "id", "subscription_id")
w7 = Window.orderBy("order_date", "id", "subscription_id").rowsBetween(-5, -1)
w_hist = Window.partitionBy(F.date_trunc("WEEK", "order_date")).orderBy("subscription_id_avg_5_prior", "id", "subscription_id").rowsBetween(-3, -1)
w_ordered = Window.partitionBy("customer_id", "store_id").orderBy(F.col("subscription_id_share").desc(), "id", "subscription_id")

matched = (
    df_orders
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.order_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

scored = (
    matched
    .select(
        "*",
        F.dense_rank().over(w).alias("discount_dense_rank"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_5"),
        F.round((F.col("subscription_id") / F.sum("subscription_id").over(w3)), 4).alias("subscription_id_share"),
        F.last("channel", ignorenulls=True).over(w4).alias("last_known_channel"),
    )
)

enriched = (
    scored
    .filter(F.col("subscription_id_share") > 0.64)
    .select(
        "*",
        F.last("channel", ignorenulls=True).over(w5).alias("last_known_channel_2"),
        F.lag("amount", 2).over(w6).alias("prev_amount"),
        F.avg("subscription_id").over(w7).alias("subscription_id_avg_5_prior"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.avg("discount_dense_rank").over(w_hist).alias("discount_dense_rank_avg_3_prior"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

result = (
    metrics
    .groupBy(
        "status",
        "customer_id",
        F.date_trunc("week", "order_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_rows"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("discount")).otherwise(0)).alias("pending_discount"),
        F.percentile_approx("subscription_id_share", 0.9).alias("subscription_id_share_p90"),
    )
    .orderBy("status")
)
