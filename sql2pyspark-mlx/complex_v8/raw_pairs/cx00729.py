from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_pct_move_type_not_in = Window.orderBy("pct_move_type_not_in_inbound_adj", "move_type").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type = Window.partitionBy("move_type")
w_all_by_pct_move_type_not_in_2 = Window.orderBy("pct_move_type_not_in_inbound_adj", "move_type")
w_all_by_prev2_min_unit_price = Window.orderBy(F.col("prev2_min_unit_price").desc_nulls_first())
w_move_type_by_min_unit_price = Window.partitionBy("move_type").orderBy(F.col("min_unit_price_share_of_move_type").desc(), "move_type")

filtered_moves = (
    df_stock_moves
    .filter(F.hour(F.col("moved_at")).between(7, 11))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws(": ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

totals = (
    filtered_moves
    .filter(F.col("operator") == "op_eve")
    .groupBy("move_type")
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.round(100 * F.count(F.when(~F.col("move_type").isin("INBOUND", "ADJUST"), 1)) / F.count("*"), 1).alias("pct_move_type_not_in_inbound_adj"),
    )
)

filtered = (
    totals
    .crossJoin(totals.agg(F.max("pct_move_type_not_in_inbound_adj").alias("overall_max_pct_move_type_not_in_inbound_adj")))
    .filter(F.col("pct_move_type_not_in_inbound_adj") < F.col("overall_max_pct_move_type_not_in_inbound_adj"))
    .drop("overall_max_pct_move_type_not_in_inbound_adj")
)

metrics = (
    filtered
    .select(
        "move_type",
        F.first("min_unit_price").over(w_all_by_pct_move_type_not_in).alias("first_min_unit_price"),
        F.round(F.col("min_unit_price") / F.nullif(F.sum("min_unit_price").over(w_move_type), F.lit(0)), 4).alias("min_unit_price_share_of_move_type"),
        F.lag("min_unit_price", 2, 0).over(w_all_by_pct_move_type_not_in_2).alias("prev2_min_unit_price"),
    )
)

sequenced = (
    metrics
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_prev2_min_unit_price), 3).alias("prev2_min_unit_price_cume_dist"),
        F.round(F.percent_rank().over(w_move_type_by_min_unit_price), 3).alias("min_unit_price_share_of_move_type_percent_rank"),
        F.round(F.col("prev2_min_unit_price") / F.nullif(F.sum("prev2_min_unit_price").over(w_move_type), F.lit(0)), 4).alias("prev2_min_unit_price_share_of_move_type"),
        F.round(F.col("first_min_unit_price") - F.avg("first_min_unit_price").over(w_move_type), 2).alias("first_min_unit_price_vs_move_type_avg"),
        F.round(F.percent_rank().over(w_all_by_prev2_min_unit_price), 3).alias("prev2_min_unit_price_percent_rank"),
        F.round(F.avg("first_min_unit_price").over(w_move_type), 2).alias("move_type_avg_first_min_unit_price"),
    )
)

result = (
    sequenced
    .filter(F.col("prev2_min_unit_price_percent_rank") > 0.2)
    .select(
        "move_type",
        "first_min_unit_price",
        "prev2_min_unit_price_cume_dist",
        "prev2_min_unit_price_share_of_move_type",
        "first_min_unit_price_vs_move_type_avg",
        "prev2_min_unit_price_percent_rank",
        "move_type_avg_first_min_unit_price",
        (F.col("prev2_min_unit_price") - F.col("first_min_unit_price")).alias("prev2_min_unit_price_minus_first_min_unit_price"),
        F.upper(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
    .orderBy(
        "first_min_unit_price_vs_move_type_avg",
        F.col("prev2_min_unit_price_minus_first_min_unit_price").desc_nulls_first(),
        "move_type",
    )
    .limit(5)
)
