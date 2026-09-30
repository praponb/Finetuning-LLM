from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id")
w_part = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "carrier")
w_recent = Window.partitionBy(F.month("delivered_date")).orderBy(F.col("delivered_date").desc(), "id")

with_windows = (
    df_shipments
    .withColumn("carrier_sum_cost", F.sum("cost").over(w))
    .withColumn("cumulative_cost", F.sum("cost").over(w_part))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w3)), 4))
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w_part))
    .withColumn("event_seq", F.row_number().over(w_recent))
)

result = (
    with_windows
    .groupBy("status")
    .agg(
        F.min("cumulative_cost").alias("min_cumulative_cost"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.round(F.sum("carrier_sum_cost") / F.nullif(F.count(F.when(F.col("status") == "RETURNED", 1)), F.lit(0)), 2).alias("carrier_sum_cost_per_returned"),
        F.sum("cost_share").alias("total_cost_share"),
    )
    .filter(F.col("total_cost_share") >= 0)
)
