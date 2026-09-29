from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.month("shipped_date")).orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier").orderBy("cost", "id")
w_grp = Window.partitionBy("order_id").orderBy("cumulative_cost", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")
w6 = Window.partitionBy(F.date_trunc("week", "shipped_date"), "carrier").orderBy("last3_avg_cumulative_cost", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.orderBy("cost", "id")
w8 = Window.partitionBy("order_id").orderBy("delivered_date", "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.avg("cost").alias("avg_shipment_cost"))

filtered = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-04-24").cast("date")) & F.col("order_id").isNotNull())
    .select("*", F.upper("status").alias("status_upper"))
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    deduped
    .withColumn("cumulative_cost", F.sum("cost").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)

metrics = (
    enriched
    .select(
        "*",
        F.avg("cumulative_cost").over(w_grp).alias("last3_avg_cumulative_cost"),
        F.count("cumulative_cost").over(w_grp).alias("last3_count_cumulative_cost"),
        F.min("cumulative_cost").over(w_grp).alias("last3_min_cumulative_cost"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.ntile(10).over(w5).alias("cost_bucket"),
        F.avg("last3_avg_cumulative_cost").over(w6).alias("last3_avg_cumulative_cost_avg_6"),
        F.row_number().over(w7).alias("seq_no_2"),
    )
)

metrics_shipments = (
    with_windows
    .select(
        "*",
        F.lead("last3_min_cumulative_cost").over(w8).alias("next_last3_min_cumulative_cost"),
        F.lag("last3_min_cumulative_cost").over(w8).alias("prev_last3_min_cumulative_cost"),
        F.lag("last3_avg_cumulative_cost_avg_6").over(w8).alias("prev_last3_avg_cumulative_cost_avg_6"),
        F.lead("last3_avg_cumulative_cost_avg_6").over(w8).alias("next_last3_avg_cumulative_cost_avg_6"),
        F.row_number().over(w8).alias("row_seq"),
    )
)

result = (
    metrics_shipments
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "status_upper",
        "cumulative_cost",
        "seq_no",
        "last3_avg_cumulative_cost",
        "last3_count_cumulative_cost",
        F.when(F.col("carrier").rlike("^(Kerry|UPS)$"), "OTHER_ZONE").when(F.col("carrier").rlike("^(DHL|FedEx)$"), "PRIORITY").otherwise("EXTENDED").alias("carrier_zone"),
        F.col("avg_shipment_cost"),
    )
    .orderBy("shipped_date")
)
