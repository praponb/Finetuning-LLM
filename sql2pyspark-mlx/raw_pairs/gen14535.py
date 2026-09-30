from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("shipped_date"), "carrier")
w2 = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w4 = Window.partitionBy("order_id")
w5 = Window.partitionBy("order_id", "status").orderBy(F.col("shipped_date_month_sum_cost").desc_nulls_last())
w6 = Window.partitionBy("order_id").orderBy(F.col("row_seq").desc_nulls_last())
w7 = Window.partitionBy("status", "order_id")

metrics = (
    df_shipments
    .select(
        "*",
        F.sum("cost").over(w).alias("shipped_date_month_sum_cost"),
        F.sum("cost").over(w2).alias("running_cost"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

matched_keys = (
    metrics
    .select("carrier")
    .exceptAll(
        df_shipments
        .filter(F.col("status") == "DELIVERED")
        .select("carrier")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.shipped_date",
        "e.delivered_date",
        "e.cost",
        "e.status",
        "e.shipped_date_month_sum_cost",
        "e.running_cost",
        "e.row_seq",
        F.col("d.discount"),
    )
)

scored = (
    enriched
    .withColumn("shipped_date_month_sum_cost_share", F.round((F.col("shipped_date_month_sum_cost") / F.sum("shipped_date_month_sum_cost").over(w4)), 4))
)

scored_shipments = (
    scored
    .withColumn("shipped_date_month_sum_cost_percent_rank", F.percent_rank().over(w5))
    .withColumn("row_seq_rank", F.rank().over(w6))
    .withColumn("shipped_date_month_sum_cost_share_2", F.round((F.col("shipped_date_month_sum_cost") / F.sum("shipped_date_month_sum_cost").over(w7)), 4))
)

joined = (
    scored_shipments
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.status").alias("order_status"), F.col("b.order_date"))
)

filtered = (
    joined
    .filter((F.col("delivered_date") >= F.lit("2023-04-25").cast("date")) & F.col("order_status").isNotNull())
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "shipped_date_month_sum_cost",
        "running_cost",
        "discount",
        "shipped_date_month_sum_cost_share",
        "shipped_date_month_sum_cost_percent_rank",
        "row_seq_rank",
        "order_status",
        "order_date",
        F.when(F.col("shipped_date_month_sum_cost") >= 457, "High").when(F.col("shipped_date_month_sum_cost") >= 193, "Medium").otherwise("Low").alias("shipped_date_month_sum_cost_tier"),
    )
)

summary = (
    filtered
    .groupBy("status")
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.sum("shipped_date_month_sum_cost_percent_rank").alias("total_shipped_date_month_sum_cost_percent_rank"),
        F.percentile_approx("shipped_date_month_sum_cost_percent_rank", 0.5).alias("shipped_date_month_sum_cost_percent_rank_p50"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
    )
)

result = (
    summary
    .select(
        "status",
        "num_status",
        "cost_stddev",
        "returned_count",
        F.when(F.col("status").rlike("^(RETURNED|DELIVERED)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy(F.col("returned_count").asc_nulls_first())
)
