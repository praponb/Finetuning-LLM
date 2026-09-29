from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date")
w2 = Window.partitionBy("carrier").orderBy(F.col("shipping_fee").desc(), "id")
w3 = Window.partitionBy(F.month("shipped_date")).orderBy(F.col("cost_whole").desc_nulls_last())
w4 = Window.partitionBy(F.date_trunc("month", "delivered_date"))
w5 = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy(F.col("cost_whole").desc_nulls_first())
w7 = Window.orderBy("cost_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_shipments
    .filter(F.col("shipped_date") > F.lit("2023-07-02").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.ceil("cost").alias("cost_whole"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_clean"),
    )
)

enriched = (
    cleaned
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id").eqNullSafe(F.col("o.id")), "left")
    .select("s.*", F.col("o.shipping_fee"), F.col("o.amount"))
)

ranked = (
    enriched
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

deduped = (
    ranked
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "cost_whole",
        "carrier_clean",
        "amount",
    )
)

enriched_shipments = (
    deduped
    .select(
        "id",
        "shipped_date",
        "cost_whole",
        "carrier_clean",
        F.dense_rank().over(w3).alias("cost_whole_dense_rank"),
        F.avg("amount").over(w4).alias("delivered_date_period_avg_amount"),
        F.sum("amount").over(w5).alias("cumulative_amount"),
        F.percent_rank().over(w_grp).alias("cost_whole_percent_rank"),
        F.sum("amount").over(w7).alias("running_amount"),
    )
)

grouped = (
    enriched_shipments
    .groupBy(
        F.date_trunc("MONTH", "shipped_date").alias("activity_month"),
        F.when(F.col("cumulative_amount") >= 8776, "PREMIUM").otherwise("STANDARD").alias("cumulative_amount_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("cost_whole_dense_rank"), 2).alias("avg_cost_whole_dense_rank"),
        F.round(F.avg("cost_whole_percent_rank"), 2).alias("avg_cost_whole_percent_rank"),
        F.percentile_approx("delivered_date_period_avg_amount", 0.9).alias("delivered_date_period_avg_amount_p90"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("cost_whole_percent_rank").alias("max_cost_whole_percent_rank"),
    )
)

result = (
    grouped
    .select(
        "activity_month",
        "cumulative_amount_tier",
        "event_count",
        "delivered_date_period_avg_amount_p90",
        "max_cost_whole_percent_rank",
    )
    .orderBy(
        F.col("activity_month").asc_nulls_first(),
        "delivered_date_period_avg_amount_p90",
    )
)
