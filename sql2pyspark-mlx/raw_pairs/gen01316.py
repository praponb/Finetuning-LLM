from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id")
w2 = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier").orderBy("shipped_date", "id")
w5 = Window.partitionBy(F.month("shipped_date"), "carrier").orderBy(F.col("next_cost").desc(), "id")
w6 = Window.orderBy("next_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("carrier").orderBy(F.col("next_cost_bucket").asc_nulls_first())
w_ordered_2 = Window.partitionBy("carrier").orderBy(F.col("next_cost_bucket").desc(), "id")
w_ordered_3 = Window.partitionBy("carrier").orderBy("cost_avg_6_prior", "id")
w10 = Window.partitionBy("carrier").orderBy(F.col("next_cost_bucket_cume_dist").desc_nulls_first(), "id")

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-02-17").cast("date")) & (~F.col("carrier").eqNullSafe(F.lit("FedEx"))))
    .withColumn("months_delivered_date_to_shipped_date", F.round((F.datediff("shipped_date", "delivered_date") / 30), 1))
)

with_windows = (
    cleaned
    .select(
        "id",
        "carrier",
        "shipped_date",
        "months_delivered_date_to_shipped_date",
        F.lead("cost", 1).over(w).alias("next_cost"),
        F.avg("cost").over(w2).alias("cost_avg_6_prior"),
        F.first("carrier", ignorenulls=True).over(w3).alias("first_known_carrier"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.lag("next_cost", 2).over(w4).alias("prev_next_cost"),
        F.ntile(10).over(w5).alias("next_cost_bucket"),
        F.sum("months_delivered_date_to_shipped_date").over(w6).alias("cumulative_months_delivered_date_to_shipped_date"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "carrier",
        "shipped_date",
        "months_delivered_date_to_shipped_date",
        "next_cost",
        "first_known_carrier",
        "prev_next_cost",
        F.cume_dist().over(w_ordered).alias("next_cost_bucket_cume_dist"),
        F.ntile(3).over(w_ordered_2).alias("next_cost_bucket_bucket"),
        F.lead("cost_avg_6_prior", 1).over(w_ordered_3).alias("next_cost_avg_6_prior"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w10))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .withColumn("next_cost_bucket_bucket_tier", F.when(F.col("next_cost_bucket_bucket") >= 9, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["carrier", "next_cost_bucket_bucket_tier"], ["carrier"]],
        "carrier",
        "next_cost_bucket_bucket_tier",
    )
    .agg(
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.sum("next_cost_bucket_bucket").alias("total_next_cost_bucket_bucket"),
        F.round(F.sum("next_cost") / F.nullif(F.count(F.when(F.col("carrier") == "DHL", 1)), F.lit(0)), 2).alias("next_cost_per_dhl"),
        F.percentile_approx("next_cost_avg_6_prior", 0.9).alias("next_cost_avg_6_prior_p90"),
        F.grouping_id().alias("gid"),
    )
)
