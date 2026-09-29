from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_operator_count_by_opene = Window.partitionBy("move_operator_count").orderBy(F.col("opened_on").desc(), "id")
w_all_by_stddev_capacity = Window.orderBy("stddev_capacity", "move_operator_count", "opened_on_quintile", "rn")
w_opened_on_quintile = Window.partitionBy("opened_on_quintile")
w_all_by_stddev_capacity_prior = Window.orderBy("stddev_capacity", "move_operator_count", "opened_on_quintile", "rn").rowsBetween(-2, -1)
w_all_by_stddev_capacity_ties = Window.orderBy("stddev_capacity")

filtered_warehouses = df_warehouses.filter(F.col("city").isNotNull())

without_matches = (
    filtered_warehouses
    .crossJoin(filtered_warehouses.agg(F.avg("capacity").alias("overall_avg_capacity")))
    .filter(F.col("capacity") > F.col("overall_avg_capacity"))
    .drop("overall_avg_capacity")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("operator") == "op_dew", 1)).alias("move_operator_count"),
    )
)

with_counts = (
    without_matches.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

with_windows = (
    with_counts
    .withColumn("rn", F.row_number().over(w_move_operator_count_by_opene))
    .withColumn("opened_on_quintile", F.ntile(5).over(w_move_operator_count_by_opene))
)

stats = (
    with_windows
    .groupBy("move_operator_count", "opened_on_quintile", "rn")
    .agg(
        F.round(F.sum("move_operator_count") / F.nullif(F.sum("opened_on_quintile"), F.lit(0)), 3).alias("move_operator_count_to_opened_on_quintile_ratio"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("capacity"), 2).alias("stddev_capacity"),
        F.max("move_operator_count").alias("max_move_operator_count"),
        F.max("opened_on_quintile").alias("max_opened_on_quintile"),
        F.sum("rn").alias("sum_rn_check"),
    )
    .filter(F.col("sum_rn_check") > 2)
    .drop("sum_rn_check")
)

ranked = (
    stats
    .select(
        "*",
        F.lag("move_operator_count", 2).over(w_all_by_stddev_capacity).alias("prev2_move_operator_count"),
        F.round(F.col("stddev_capacity") - F.avg("stddev_capacity").over(w_opened_on_quintile), 2).alias("stddev_capacity_vs_opened_on_quintile_avg"),
        F.max("move_operator_count").over(w_opened_on_quintile).alias("opened_on_quintile_max_move_operator_count"),
        F.row_number().over(w_all_by_stddev_capacity).alias("rn_2"),
        F.max("row_count").over(w_all_by_stddev_capacity_prior).alias("row_count_max_2_prior"),
        F.rank().over(w_all_by_stddev_capacity_ties).alias("stddev_capacity_rank"),
    )
)

derived = (
    ranked
    .filter(F.col("stddev_capacity_vs_opened_on_quintile_avg") > -14000.0)
    .select(
        "move_operator_count",
        "opened_on_quintile",
        "rn",
        "move_operator_count_to_opened_on_quintile_ratio",
        "stddev_capacity",
        "max_opened_on_quintile",
        "stddev_capacity_vs_opened_on_quintile_avg",
        "opened_on_quintile_max_move_operator_count",
        "rn_2",
        "row_count_max_2_prior",
        F.round(F.col("stddev_capacity") * 0.9, 2).alias("stddev_capacity_adj"),
        F.when(F.col("opened_on_quintile") >= 9, "A").when(F.col("opened_on_quintile") >= 3, "B").otherwise("C").alias("opened_on_quintile_tier"),
        F.coalesce(F.col("row_count_max_2_prior"), F.lit(0)).alias("row_count_max_2_prior_filled"),
    )
)

result = (
    derived
    .filter(F.col("opened_on_quintile_tier") == "A")
    .select(
        "move_operator_count",
        "opened_on_quintile",
        "move_operator_count_to_opened_on_quintile_ratio",
        "stddev_capacity",
        "max_opened_on_quintile",
        "stddev_capacity_vs_opened_on_quintile_avg",
        "opened_on_quintile_max_move_operator_count",
        "row_count_max_2_prior",
        "stddev_capacity_adj",
        "row_count_max_2_prior_filled",
        F.coalesce(F.col("opened_on_quintile_tier"), F.lit("UNKNOWN")).alias("opened_on_quintile_tier_or_unknown"),
        F.abs(F.col("max_opened_on_quintile") - 6).alias("max_opened_on_quintile_distance"),
    )
    .orderBy(
        F.col("stddev_capacity_adj").asc_nulls_last(),
        F.col("row_count_max_2_prior").asc(),
    )
)
