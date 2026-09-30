from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("delivered_date").asc_nulls_last())
w2 = Window.partitionBy("order_id").orderBy("cost", "id")
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(-3, -1)
w6 = Window.partitionBy("status")
w7 = Window.orderBy("running_cost", "id").rowsBetween(-2, -1)
w8 = Window.partitionBy("status").orderBy("cost_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("status").orderBy("cost_bucket", "id")
w10 = Window.orderBy("running_cost", "id")
w11 = Window.orderBy(F.col("status_avg_cost_avg_3_prior").desc_nulls_last())
w12 = Window.partitionBy("status").orderBy("status_avg_cost_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_shipments
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "cost",
        "status",
        F.ntile(5).over(w2).alias("cost_bucket"),
        F.round((F.col("cost") / F.sum("cost").over(w3)), 4).alias("cost_share"),
        F.sum("cost").over(w4).alias("running_cost"),
        F.avg("cost").over(w5).alias("cost_avg_3_prior"),
    )
)

metrics_shipments = (
    metrics
    .withColumn("status_avg_cost_avg_3_prior", F.avg("cost_avg_3_prior").over(w6))
    .withColumn("cost_avg_3_prior_avg_2_prior", F.avg("cost_avg_3_prior").over(w7))
    .withColumn("last_known_cost_share", F.last("cost_share", ignorenulls=True).over(w8))
    .withColumn("event_seq", F.row_number().over(w9))
)

scored = (
    metrics_shipments
    .select(
        "id",
        "status",
        "cost_bucket",
        "cost_share",
        "running_cost",
        "cost_avg_3_prior_avg_2_prior",
        "last_known_cost_share",
        F.max("event_seq").over(w6).alias("status_max_event_seq"),
        F.lag("cost_avg_3_prior", 1).over(w10).alias("prev_cost_avg_3_prior"),
        F.cume_dist().over(w11).alias("status_avg_cost_avg_3_prior_cume_dist"),
        F.sum("cost_avg_3_prior").over(w12).alias("cumulative_cost_avg_3_prior"),
        F.sum("status_avg_cost_avg_3_prior").over(w13).alias("running_status_avg_cost_avg_3_prior"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("cost_avg_3_prior_avg_2_prior") >= 97, "PREMIUM").otherwise("STANDARD").alias("cost_avg_3_prior_avg_2_prior_tier"),
        "status",
        F.when(F.col("last_known_cost_share") >= 0.71, "HIGH").otherwise("LOW").alias("last_known_cost_share_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("last_known_cost_share", [0.25, 0.5, 0.75]).alias("last_known_cost_share_quartiles"),
    )
    .filter(F.col("num_status") > 1)
    .orderBy(F.col("status").desc())
)
