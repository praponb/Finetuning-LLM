from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_first())
w3 = Window.partitionBy("order_id").orderBy(F.col("last7_count_amount").desc())
w4 = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")
w5 = Window.partitionBy(F.date_trunc("week", "shipped_date"), "order_id")
w6 = Window.partitionBy("order_id").orderBy("last7_min_amount", "id")
w7 = Window.orderBy("cost_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id").eqNullSafe(F.col("p.id")), "left")
    .select("e.*", F.col("p.order_date"), F.col("p.amount"))
)

cleaned = (
    with_dims
    .filter(F.col("order_id").isNotNull() & (F.col("shipped_date") >= F.to_date(F.lit("2023-06-28"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "amount",
        F.upper("status").alias("status_upper"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("carrier") == "Kerry")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        F.min("amount").over(w).alias("last7_min_amount"),
        F.count("amount").over(w).alias("last7_count_amount"),
        F.avg("amount").over(w).alias("last7_avg_amount"),
        F.sum("amount").over(w).alias("last7_sum_amount"),
    )
)

enriched = (
    with_windows
    .withColumn("cost_percent_rank", F.percent_rank().over(w2))
    .withColumn("last7_count_amount_percent_rank", F.percent_rank().over(w3))
    .withColumn("cost_bucket", F.ntile(4).over(w4))
    .withColumn("shipped_date_period_min_last7_sum_amount", F.min("last7_sum_amount").over(w5))
)

enriched_shipments = (
    enriched
    .select(
        "*",
        F.lead("last7_sum_amount").over(w6).alias("next_last7_sum_amount"),
        F.lag("last7_sum_amount").over(w6).alias("prev_last7_sum_amount"),
        F.lead("last7_avg_amount").over(w6).alias("next_last7_avg_amount"),
        F.lag("last7_avg_amount").over(w6).alias("prev_last7_avg_amount"),
        F.row_number().over(w6).alias("row_seq"),
        F.sum("shipped_date_period_min_last7_sum_amount").over(w7).alias("running_shipped_date_period_min_last7_sum_amount"),
    )
)

result = (
    enriched_shipments
    .select(
        "id",
        "order_id",
        "cost",
        "last7_min_amount",
        "last7_count_amount",
        "last7_avg_amount",
        "cost_percent_rank",
        "last7_count_amount_percent_rank",
        "cost_bucket",
    )
)
