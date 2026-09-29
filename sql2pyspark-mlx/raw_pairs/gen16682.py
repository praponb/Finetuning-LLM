from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "shipped_date"), "order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("cost", "id")
w3 = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")
w4 = Window.partitionBy("carrier").orderBy("cost", "id")
w_part = Window.partitionBy(F.date_trunc("MONTH", "shipped_date")).orderBy(F.col("row_seq_2").desc_nulls_last())
w6 = Window.partitionBy("status").orderBy("row_seq", "id").rowsBetween(-5, Window.currentRow)
w_hist = Window.partitionBy("status").orderBy("delivered_date", "id")
w8 = Window.partitionBy("order_id").orderBy(F.col("row_seq_2_avg_5").desc(), "id")
w9 = Window.partitionBy("order_id").orderBy("row_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("running_max_row_seq_2_cume_dist", "id")
w11 = Window.orderBy("event_seq_3", "id")
w12 = Window.orderBy("running_avg_row_seq_2_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy("row_seq", "id")
w14 = Window.orderBy("event_seq_3", "id").rowsBetween(-2, Window.currentRow)

scored = (
    df_shipments
    .filter(F.col("carrier").isin(["FedEx", "UPS"]))
    .withColumn("first_known_carrier", F.first("carrier", ignorenulls=True).over(w))
    .withColumn("cost_bucket", F.ntile(4).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("row_seq_2", F.row_number().over(w4))
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

filtered = (
    latest
    .filter(~F.col("carrier").eqNullSafe(F.lit("Kerry")))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "first_known_carrier",
        "cost_bucket",
        "row_seq",
        "row_seq_2",
        "rn",
        F.lower("carrier").alias("carrier_lower"),
        F.round("row_seq_2", 2).alias("row_seq_2_rounded"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "cost_bucket",
        "row_seq",
        "row_seq_2",
        "rn",
        "carrier_clean",
        F.cume_dist().over(w_part).alias("row_seq_2_cume_dist"),
        F.avg("row_seq_2").over(w6).alias("row_seq_2_avg_5"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.row_number().over(w3).alias("event_seq_2"),
    )
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
)

with_windows_shipments = (
    deduped
    .select(
        "id",
        "cost",
        "row_seq",
        "row_seq_2",
        "rn",
        "row_seq_2_avg_5",
        F.avg("row_seq_2_cume_dist").over(w9).alias("running_avg_row_seq_2_cume_dist"),
        F.sum("row_seq_2_cume_dist").over(w9).alias("running_sum_row_seq_2_cume_dist"),
        F.max("row_seq_2_cume_dist").over(w9).alias("running_max_row_seq_2_cume_dist"),
        F.row_number().over(w8).alias("event_seq_3"),
    )
)

metrics = (
    with_windows_shipments
    .select(
        "id",
        "cost",
        "row_seq_2",
        "rn",
        "row_seq_2_avg_5",
        "running_avg_row_seq_2_cume_dist",
        "event_seq_3",
        F.ntile(10).over(w10).alias("running_max_row_seq_2_cume_dist_bucket"),
        F.ntile(10).over(w11).alias("event_seq_3_bucket"),
        F.first("row_seq_2", ignorenulls=True).over(w12).alias("first_known_row_seq_2"),
        F.lag("row_seq_2_avg_5", 1).over(w13).alias("prev_row_seq_2_avg_5"),
        F.avg("cost").over(w14).alias("cost_avg_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "row_seq_2",
        "rn",
        "event_seq_3",
        "running_max_row_seq_2_cume_dist_bucket",
        "event_seq_3_bucket",
        "first_known_row_seq_2",
        "prev_row_seq_2_avg_5",
        "cost_avg_2",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
