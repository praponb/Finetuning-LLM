from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date_period").desc_nulls_last(), "id")
w2 = Window.orderBy("shipped_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("shipped_date"), "status").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.month("delivered_date"), "status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc_nulls_first())
w6 = Window.partitionBy("status").orderBy("shipped_date", "id")
w7 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.year("delivered_date"), "carrier")
w9 = Window.partitionBy("status").orderBy("shipped_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("carrier").orderBy("delivered_date_year_sum_seq_no", "id")
w11 = Window.partitionBy("carrier").orderBy(F.col("delivered_date_year_sum_seq_no").desc_nulls_first())

filtered = (
    df_shipments
    .filter(F.col("carrier") != "FedEx")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        F.date_trunc("month", "shipped_date").alias("shipped_date_period"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .withColumn("first_known_carrier", F.first("carrier", ignorenulls=True).over(w2))
    .withColumn("first_known_carrier_2", F.first("carrier", ignorenulls=True).over(w3))
    .withColumn("first_known_carrier_3", F.first("carrier", ignorenulls=True).over(w4))
)

ranked = (
    metrics
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

metrics_shipments = (
    ranked
    .filter(F.hour("shipped_date_period").between(8, 12))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "shipped_date_period",
        "first_known_carrier",
        "first_known_carrier_3",
        F.row_number().over(w6).alias("seq_no"),
        F.last("carrier", ignorenulls=True).over(w7).alias("last_known_carrier"),
    )
)

with_windows = (
    metrics_shipments
    .select(
        "id",
        "carrier",
        "shipped_date_period",
        "first_known_carrier_3",
        F.sum("seq_no").over(w8).alias("delivered_date_year_sum_seq_no"),
        F.sum("seq_no").over(w9).alias("running_seq_no"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.lead("delivered_date_year_sum_seq_no").over(w10).alias("next_delivered_date_year_sum_seq_no"),
        F.lag("delivered_date_year_sum_seq_no").over(w10).alias("prev_delivered_date_year_sum_seq_no"),
        F.lag("running_seq_no").over(w10).alias("prev_running_seq_no"),
        F.lead("running_seq_no").over(w10).alias("next_running_seq_no"),
        F.row_number().over(w10).alias("row_seq"),
        F.cume_dist().over(w11).alias("delivered_date_year_sum_seq_no_cume_dist"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("row_seq") >= 7, "HIGH").otherwise("LOW").alias("row_seq_tier"),
    )
    .agg(
        F.avg(F.when(F.col("carrier") == "DHL", F.col("running_seq_no"))).alias("dhl_avg_running_seq_no"),
        F.sum("delivered_date_year_sum_seq_no_cume_dist").alias("total_delivered_date_year_sum_seq_no_cume_dist"),
        F.count("*").alias("event_count"),
    )
)
