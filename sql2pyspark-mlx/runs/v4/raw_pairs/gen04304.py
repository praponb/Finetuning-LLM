from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "order_id").orderBy(F.col("amount_int").desc())
w4 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("method").orderBy("amount_int", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("method", "order_id").orderBy(F.col("paid_at").desc(), "id")
w7 = Window.partitionBy("channel").orderBy("paid_at", "id")
w8 = Window.partitionBy("channel").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("order_id").orderBy(F.col("new_group_flag").desc_nulls_last(), "id")

valid = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select("*", F.col("amount").cast("int").alias("amount_int"))
)

enriched = (
    valid
    .withColumn("amount_rank", F.rank().over(w))
    .withColumn("first_known_amount_int", F.first("amount_int", ignorenulls=True).over(w2))
    .withColumn("amount_int_percent_rank", F.percent_rank().over(w3))
    .withColumn("amount_int_max_30d", F.max("amount_int").over(w4))
    .withColumn("amount_avg_3", F.avg("amount").over(w5))
)

joined = (
    enriched
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.channel"))
)

ranked = (
    joined
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

gaps = (
    ranked
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w7).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w7) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("new_group_flag").over(w8))

ranked_payments = (
    islands
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
)

result = (
    ranked_payments
    .withColumn("week_start", F.date_trunc("WEEK", "paid_at"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 1600, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["week_start", "amount_tier"], ["week_start"]],
        "week_start",
        "amount_tier",
    )
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.percentile_approx("session_no", 0.5).alias("session_no_p50"),
        F.avg(F.when(F.col("channel") == "mobile", F.col("amount"))).alias("mobile_avg_amount"),
        F.max(F.when(F.col("status") == "FAILED", F.col("amount_avg_3"))).alias("failed_max_amount_avg_3"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("session_no_p50") > 0)
)
