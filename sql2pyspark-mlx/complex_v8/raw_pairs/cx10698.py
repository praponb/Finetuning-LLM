from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_median_csat_priority = Window.partitionBy("median_csat", "priority")
w_all_by_median_csat_full = Window.orderBy(F.col("median_csat").desc(), "priority").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_median_csat_prior_2 = Window.orderBy(F.col("median_csat").desc(), "priority").rowsBetween(-2, -1)
w_all_by_median_csat = Window.orderBy(F.col("median_csat").desc(), "priority")

recent_tickets = df_tickets.filter((F.col("priority").isin("P2", "P4")) & (F.col("csat") >= 2))

grouped = (
    recent_tickets
    .groupBy("priority")
    .agg(
        F.round(F.sum("requester_id") / F.nullif(F.sum("csat"), F.lit(0)), 3).alias("requester_id_to_csat_ratio"),
        F.percentile_approx("csat", 0.5).alias("median_csat"),
        F.count("*").alias("n_rows"),
        F.sum("requester_id").alias("sum_requester_id_check"),
    )
    .filter(F.col("sum_requester_id_check") > 8)
    .drop("sum_requester_id_check")
)

enriched = (
    grouped
    .withColumn("priority_ref", F.concat(F.col("priority"), F.lit("#"), F.col("n_rows").cast("string")))
)

windowed = (
    enriched
    .select(
        "priority",
        "requester_id_to_csat_ratio",
        "median_csat",
        "n_rows",
        "priority_ref",
        F.round(F.col("n_rows") / F.nullif(F.sum("n_rows").over(w_median_csat_priority), F.lit(0)), 4).alias("n_rows_share_of_median_csat_priority"),
        F.last("requester_id_to_csat_ratio").over(w_all_by_median_csat_full).alias("last_requester_id_to_csat_ratio"),
        F.max("requester_id_to_csat_ratio").over(w_all_by_median_csat_prior_2).alias("requester_id_to_csat_ratio_max_2_prior"),
        (F.col("median_csat") - F.lag("median_csat").over(w_all_by_median_csat)).alias("median_csat_change"),
        F.dense_rank().over(w_all_by_median_csat).alias("median_csat_dense_rank"),
    )
)

result = (
    windowed
    .groupBy("median_csat_dense_rank")
    .pivot("priority", ["P4", "P1", "P2"])
    .agg(F.count("median_csat"))
    .orderBy("median_csat_dense_rank")
)
