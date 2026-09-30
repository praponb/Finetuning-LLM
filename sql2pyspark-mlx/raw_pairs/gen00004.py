from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost_int").asc_nulls_last())
w2 = Window.partitionBy("carrier").orderBy("cost_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("cost_int", "id")
w_hist = Window.partitionBy("carrier").orderBy("cumulative_cost_int", "id")
w5 = Window.partitionBy("carrier").orderBy("next_cost_int_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("event_seq").desc(), "id")
w7 = Window.partitionBy("carrier").orderBy("cost_int_rank", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-07-23").cast("date")) & (F.col("carrier").eqNullSafe(F.lit("UPS"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.concat("status", F.lit("/"), "carrier").alias("status_carrier"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

base = (
    cleaned
    .filter(F.col("shipped_date") >= F.lit("2023-02-21").cast("date"))
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "status",
        "status_carrier",
        F.col("cost").cast("int").alias("cost_int"),
    )
)

metrics = (
    base
    .select(
        "id",
        "carrier",
        "status",
        "status_carrier",
        F.rank().over(w).alias("cost_int_rank"),
        F.sum("cost_int").over(w2).alias("cumulative_cost_int"),
        F.lag("cost_int", 1).over(w3).alias("prev_cost_int"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.lead("prev_cost_int").over(w_hist).alias("next_prev_cost_int"),
        F.lag("prev_cost_int").over(w_hist).alias("prev_prev_cost_int"),
        F.lead("cost_int_rank").over(w_hist).alias("next_cost_int_rank"),
        F.lag("cost_int_rank").over(w_hist).alias("prev_cost_int_rank"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

enriched = (
    with_windows
    .withColumn("first_known_next_prev_cost_int", F.first("next_prev_cost_int", ignorenulls=True).over(w5))
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("cumulative_cost_int_avg_6", F.avg("cumulative_cost_int").over(w7))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("cumulative_cost_int") >= 305, "HIGH").otherwise("LOW").alias("cumulative_cost_int_tier"),
        "carrier",
        F.when(F.col("cumulative_cost_int_avg_6") >= 380, "HIGH").otherwise("LOW").alias("cumulative_cost_int_avg_6_tier"),
    )
    .agg(
        F.count_if(F.col("status") == "DELIVERED").alias("delivered_events"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("row_seq"))).alias("delivered_total_row_seq"),
    )
    .orderBy(F.col("in_transit_count").desc_nulls_first())
)
