from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc())
w2 = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")
w3 = Window.orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("row_seq", "id")
w7 = Window.orderBy(F.col("row_seq").desc(), "id")
w_ordered = Window.orderBy("row_seq", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_shipments
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "carrier", "shipped_date", "status")
)

filtered = (
    latest
    .filter(F.col("carrier").isNotNull())
    .withColumn("carrier_status_key", F.concat_ws("-", "carrier", "status"))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("OTHER")))
)

enriched = (
    filtered
    .filter(F.month("shipped_date").isin([2, 5, 8]))
    .select(
        "id",
        "shipped_date",
        F.row_number().over(w2).alias("row_seq"),
        F.first("carrier", ignorenulls=True).over(w3).alias("first_known_carrier"),
        F.last("carrier", ignorenulls=True).over(w4).alias("last_known_carrier"),
        F.first("carrier", ignorenulls=True).over(w5).alias("first_known_carrier_2"),
    )
)

enriched_shipments = (
    enriched
    .select(
        "*",
        F.lag("row_seq").over(w_grp).alias("prev_row_seq"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.ntile(3).over(w7).alias("row_seq_bucket"),
    )
)

filtered_shipments = (
    enriched_shipments
    .filter((F.col("shipped_date") > F.lit("2023-03-04").cast("date")) & (F.col("prev_row_seq") > 5))
    .select(
        "id",
        "shipped_date",
        "row_seq",
        "first_known_carrier",
        "last_known_carrier",
        "first_known_carrier_2",
        "prev_row_seq",
        "event_seq",
        "row_seq_bucket",
        F.greatest("prev_row_seq", "row_seq").alias("greatest_prev_row_seq_row_seq"),
    )
)

metrics = (
    filtered_shipments
    .select(
        "id",
        "shipped_date",
        "first_known_carrier",
        "greatest_prev_row_seq_row_seq",
        F.count("prev_row_seq").over(w_ordered).alias("last7_count_prev_row_seq"),
        F.sum("prev_row_seq").over(w_ordered).alias("last7_sum_prev_row_seq"),
        F.avg("prev_row_seq").over(w_ordered).alias("last7_avg_prev_row_seq"),
        F.max("prev_row_seq").over(w_ordered).alias("last7_max_prev_row_seq"),
        F.last("event_seq", ignorenulls=True).over(w9).alias("last_known_event_seq"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("last7_max_prev_row_seq") >= 7.0, "LARGE").otherwise("SMALL").alias("last7_max_prev_row_seq_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("greatest_prev_row_seq_row_seq", 0.9).alias("greatest_prev_row_seq_row_seq_p90"),
    )
    .orderBy(F.col("distinct_ids").desc())
)
