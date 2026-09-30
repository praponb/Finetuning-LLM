from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("delivered_date"), "status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("weeks_delivered_date_to_shipped_date").desc(), "id")
w3 = Window.partitionBy(F.year("shipped_date"), "carrier").orderBy("first_known_cost", "id")
w4 = Window.partitionBy("order_id")
w5 = Window.partitionBy(F.date_trunc("WEEK", "shipped_date"), "status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.month("delivered_date"), "carrier").orderBy("weeks_delivered_date_to_shipped_date", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy("carrier").orderBy(F.col("first_known_cost_share").desc_nulls_first())
w8 = Window.partitionBy("carrier").orderBy("row_num", "id")
w9 = Window.partitionBy("carrier").orderBy("first_known_weeks_delivered_date_to_shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-05-08").cast("date")) & (F.col("carrier").eqNullSafe(F.lit("FedEx"))))
    .withColumn("weeks_delivered_date_to_shipped_date", (F.datediff("shipped_date", "delivered_date") / 7))
    .withColumn("is_in_transit", F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0))
)

with_windows = (
    filtered
    .select(
        "*",
        F.first("cost", ignorenulls=True).over(w).alias("first_known_cost"),
        F.ntile(5).over(w2).alias("weeks_delivered_date_to_shipped_date_bucket"),
    )
)

with_windows_shipments = (
    with_windows
    .filter(F.col("status") == "DELIVERED")
    .select(
        "id",
        "carrier",
        "delivered_date",
        "weeks_delivered_date_to_shipped_date",
        F.lead("first_known_cost", 1).over(w3).alias("next_first_known_cost"),
        F.round((F.col("first_known_cost") / F.sum("first_known_cost").over(w4)), 4).alias("first_known_cost_share"),
        F.first("weeks_delivered_date_to_shipped_date", ignorenulls=True).over(w5).alias("first_known_weeks_delivered_date_to_shipped_date"),
        F.avg("is_in_transit").over(w6).alias("is_in_transit_avg_6_prior"),
    )
)

deduped = (
    with_windows_shipments
    .withColumn("row_num", F.rank().over(w7))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped
    .select(
        "id",
        "delivered_date",
        "next_first_known_cost",
        "first_known_cost_share",
        "first_known_weeks_delivered_date_to_shipped_date",
        "row_num",
        F.lag("weeks_delivered_date_to_shipped_date").over(w8).alias("prev_weeks_delivered_date_to_shipped_date"),
        F.lag("is_in_transit_avg_6_prior").over(w8).alias("prev_is_in_transit_avg_6_prior"),
        F.lead("is_in_transit_avg_6_prior").over(w8).alias("next_is_in_transit_avg_6_prior"),
        F.row_number().over(w8).alias("row_seq"),
        F.sum("next_first_known_cost").over(w9).alias("cumulative_next_first_known_cost"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("prev_weeks_delivered_date_to_shipped_date") >= 88, "LARGE").otherwise("SMALL").alias("prev_weeks_delivered_date_to_shipped_date_tier"),
        F.when(F.col("row_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
    )
    .agg(
        F.percentile_approx("first_known_cost_share", 0.9).alias("first_known_cost_share_p90"),
        F.sum("cumulative_next_first_known_cost").alias("total_cumulative_next_first_known_cost"),
        F.count("*").alias("event_count"),
    )
    .orderBy("event_count")
)
