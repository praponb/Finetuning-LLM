from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("carrier")
w3 = Window.orderBy("cost", "id")

with_windows = (
    df_shipments
    .filter(F.month("shipped_date").isin([3, 8, 12]))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.avg("cost").over(w).alias("cost_avg_3_prior"),
        F.sum("cost").over(w2).alias("carrier_sum_cost"),
        F.lead("cost", 1, 0).over(w3).alias("next_cost"),
    )
)

result = (
    with_windows
    .cube("carrier")
    .agg(
        F.count_if(F.col("carrier") == "Kerry").alias("kerry_events"),
        F.sum(F.when(F.col("carrier") == "DHL", F.col("cost")).otherwise(0)).alias("dhl_cost"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.countDistinct("status").alias("distinct_status"),
        F.count("*").alias("row_count"),
        F.grouping("carrier").alias("grouping_id"),
    )
)
