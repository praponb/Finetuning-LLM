from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_to_date_by_moved_at = Window.partitionBy(F.to_date("moved_at")).orderBy(F.col("moved_at_month").desc(), "id")
w_moved_at_month_operator_grou = Window.partitionBy(F.month("moved_at"), "operator_group_group")
w_first_moved_at_month_by_move = Window.partitionBy(F.month("first_moved_at")).orderBy("moved_at_month_operator_group_group_min_moved_at_2")
w_first_moved_at_month_by_firs = Window.partitionBy(F.date_trunc("month", "first_moved_at")).orderBy("first_moved_at")
w_first_moved_at_month_by_move_2 = Window.partitionBy(F.month("first_moved_at")).orderBy("moved_at_month_operator_group_group_min_moved_at_2", "moved_at_day")
w_moved_at_month_operator_grou_2 = Window.partitionBy("moved_at_month_operator_group_group_min_moved_at_3")
w_all_by_moved_at_month_operat = Window.orderBy(F.col("moved_at_month_operator_group_group_min_moved_at").desc())

filtered = (
    df_stock_moves
    .filter((F.col("qty") <= 332) & (F.month(F.col("moved_at")).isin(2, 3)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.when(F.col("operator").isin("op_ann", "op_eve"), "group_a").when(F.col("operator") == "op_dew", "group_b").otherwise("other").alias("operator_group"),
    )
)

annotated = (
    filtered
    .select(
        "id",
        "move_type",
        "moved_at",
        "operator_group",
        F.month(F.col("moved_at")).alias("moved_at_month"),
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
        F.when(F.col("operator_group").isin("group_a", "other"), "group_a").when(F.col("operator_group") == "group_b", "group_b").otherwise("other").alias("operator_group_group"),
    )
)

metrics = (
    annotated
    .select(
        "id",
        "moved_at",
        "operator_group",
        "moved_at_month",
        "move_type_prefix",
        "operator_group_group",
        F.lead("moved_at_month", 2).over(w_moved_at_to_date_by_moved_at).alias("next2_moved_at_month"),
        F.round(F.col("moved_at_month") - F.avg("moved_at_month").over(w_moved_at_month_operator_grou), 2).alias("moved_at_month_vs_moved_at_month_operator_group_"),
        F.min("moved_at_month").over(w_moved_at_month_operator_grou).alias("moved_at_month_operator_group_group_min_moved_at"),
    )
)

rollup_base = (
    metrics
    .filter(~F.col("operator_group").isin("other", "group_a"))
    .groupBy(F.to_date("moved_at").alias("moved_at_day"))
    .agg(
        F.round(F.sum("moved_at_month_operator_group_group_min_moved_at") / F.nullif(F.sum("moved_at_month_vs_moved_at_month_operator_group_"), F.lit(0)), 3).alias("moved_at_month_operator_group_group_min_moved_at_2"),
        F.min("moved_at").alias("first_moved_at"),
        F.max("moved_at_month_vs_moved_at_month_operator_group_").alias("max_moved_at_month_vs_moved_at_month_operator_gr"),
    )
    .filter(F.col("max_moved_at_month_vs_moved_at_month_operator_gr") > -8.4)
    .drop("max_moved_at_month_vs_moved_at_month_operator_gr")
)

with_trends = (
    rollup_base
    .select(
        "*",
        F.round(F.cume_dist().over(w_first_moved_at_month_by_move), 3).alias("moved_at_month_operator_group_group_min_moved_at"),
        F.round(F.percent_rank().over(w_first_moved_at_month_by_firs), 3).alias("first_moved_at_percent_rank"),
        F.rank().over(w_first_moved_at_month_by_move_2).alias("moved_at_month_operator_group_group_min_moved_at_3"),
    )
)

metrics_2 = (
    with_trends
    .select(
        "*",
        F.round(F.col("first_moved_at_percent_rank") / F.nullif(F.sum("first_moved_at_percent_rank").over(w_moved_at_month_operator_grou_2), F.lit(0)), 4).alias("first_moved_at_percent_rank_share_of_moved_at_mo"),
        F.round(F.cume_dist().over(w_all_by_moved_at_month_operat), 3).alias("moved_at_month_operator_group_group_min_moved_at_4"),
    )
)

without_matches = (
    metrics_2
    .crossJoin(metrics_2.agg(F.min("first_moved_at_percent_rank_share_of_moved_at_mo").alias("overall_min_first_moved_at_percent_rank_share_of")))
    .filter(F.col("first_moved_at_percent_rank_share_of_moved_at_mo") > F.col("overall_min_first_moved_at_percent_rank_share_of"))
    .drop("overall_min_first_moved_at_percent_rank_share_of")
)

result = (
    without_matches
    .select(
        "moved_at_day",
        "moved_at_month_operator_group_group_min_moved_at_2",
        "first_moved_at",
        "first_moved_at_percent_rank",
        "moved_at_month_operator_group_group_min_moved_at_3",
        "first_moved_at_percent_rank_share_of_moved_at_mo",
        "moved_at_month_operator_group_group_min_moved_at_4",
        F.month(F.col("moved_at_day")).alias("moved_at_day_month"),
    )
    .orderBy(
        F.col("first_moved_at").desc(),
        "moved_at_month_operator_group_group_min_moved_at_2",
        "moved_at_day",
    )
    .limit(5)
)
