from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("order_id", "status").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("status").orderBy("shipped_date", "id")
w5 = Window.partitionBy("order_id", "carrier").orderBy(F.col("cost").desc_nulls_last())
w6 = Window.partitionBy("carrier").orderBy(F.col("amount").desc(), "id")
w_hist = Window.partitionBy("carrier")
w8 = Window.partitionBy("carrier").orderBy("order_id_max_carrier_length", "id")
w9 = Window.partitionBy("order_id").orderBy("cumulative_carrier_length", "id")
w_ordered = Window.partitionBy("channel").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("channel").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("carrier", "order_id")
w13 = Window.orderBy(F.col("carrier_max_amount_bucket").desc(), "id")
w14 = Window.partitionBy("carrier").orderBy("cost_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w15 = Window.partitionBy("channel").orderBy("seq_no_2", "id")

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .withColumn("carrier_length", F.length("carrier"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("NONE")))
)

with_dims = (
    valid
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.channel"), F.col("b.amount"), F.col("b.order_date"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.first("cost", ignorenulls=True).over(w_recent).alias("first_known_cost"),
        F.ntile(3).over(w_grp).alias("amount_bucket"),
        F.sum("carrier_length").over(w_recent).alias("cumulative_carrier_length"),
    )
)

scored = (
    with_windows
    .withColumn("order_id_max_carrier_length", F.max("carrier_length").over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("cost_percent_rank", F.percent_rank().over(w5))
    .withColumn("seq_no_2", F.row_number().over(w6))
)

scored_shipments = (
    scored
    .withColumn("carrier_max_amount_bucket", F.max("amount_bucket").over(w_hist))
    .withColumn("order_id_max_carrier_length_bucket", F.ntile(5).over(w8))
    .withColumn("event_seq", F.row_number().over(w9))
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w_ordered))
    .withColumn("first_known_first_known_cost", F.first("first_known_cost", ignorenulls=True).over(w_running))
)

with_windows_shipments = (
    scored_shipments
    .withColumn("carrier_max_carrier_length", F.max("carrier_length").over(w12))
    .withColumn("event_seq_2", F.row_number().over(w13))
)

scored_shipments_2 = (
    with_windows_shipments
    .select(
        "*",
        F.round((F.col("seq_no_2") / F.sum("seq_no_2").over(w3)), 4).alias("seq_no_2_share"),
        F.last("cost_percent_rank", ignorenulls=True).over(w14).alias("last_known_cost_percent_rank"),
        F.lag("cumulative_carrier_length", 2).over(w15).alias("prev_cumulative_carrier_length"),
    )
)

result = (
    scored_shipments_2
    .withColumn("seq_no_2_tier", F.when(F.col("seq_no_2") >= 8, "PREMIUM").otherwise("STANDARD"))
    .withColumn("first_known_first_known_cost_tier", F.when(F.col("first_known_first_known_cost") >= 101, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["seq_no_2_tier", "first_known_first_known_cost_tier"], ["seq_no_2_tier"]],
        "seq_no_2_tier",
        "first_known_first_known_cost_tier",
    )
    .agg(
        F.round(F.sum("last_known_cost_percent_rank") / F.nullif(F.count(F.when(F.col("status") == "IN_TRANSIT", 1)), F.lit(0)), 2).alias("last_known_cost_percent_rank_per_in_transit"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_rows"),
        F.grouping_id().alias("grouping_level"),
    )
)
