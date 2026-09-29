from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt_tier_by_n_move_type_adju = Window.partitionBy("cnt_tier").orderBy(F.col("n_move_type_adjust").desc(), "moved_at_day")
w_cnt_tier_by_n_move_type_adju_2 = Window.partitionBy("cnt_tier").orderBy(F.col("n_move_type_adjust").desc())
w_n_operator_in_op_bee_op_chan = Window.partitionBy("n_operator_in_op_bee_op_chan_o", "moved_at_day_year")

prepared_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
    )
)

grouped = (
    prepared_moves
    .groupBy(F.to_date("moved_at").alias("moved_at_day"))
    .agg(
        F.count(F.when(F.col("operator").isin("op_bee", "op_chan", "op_eve"), 1)).alias("n_operator_in_op_bee_op_chan_o"),
        F.sum(F.when(F.col("operator").isNotNull(), F.col("unit_price")).otherwise(0)).alias("unit_price_operator_is_not_null"),
        F.max("moved_at").alias("last_moved_at"),
        F.min("unit_price").alias("min_unit_price"),
        F.sum(F.when(F.col("move_type") == "ADJUST", 1).otherwise(0)).alias("n_move_type_adjust"),
        F.count("*").alias("cnt"),
    )
)

annotated = (
    grouped
    .filter(F.col("min_unit_price").between(25.9, 450.1))
    .select(
        "*",
        F.when(F.col("cnt") >= 10, "HIGH").when(F.col("cnt") >= 6, "MEDIUM").otherwise("LOW").alias("cnt_tier"),
        F.year(F.col("moved_at_day")).alias("moved_at_day_year"),
    )
)

metrics = (
    annotated
    .select(
        "*",
        F.lag("moved_at_day").over(w_cnt_tier_by_n_move_type_adju).alias("prev_moved_at_day"),
        F.dense_rank().over(w_cnt_tier_by_n_move_type_adju_2).alias("n_move_type_adjust_dense_rank"),
        F.datediff(F.col("moved_at_day"), F.lag("moved_at_day").over(w_cnt_tier_by_n_move_type_adju)).alias("days_since_prev_moved_at_day"),
        F.min("moved_at_day_year").over(w_n_operator_in_op_bee_op_chan).alias("n_operator_in_op_bee_op_chan_o_moved_at_day_year"),
        F.sum("moved_at_day_year").over(w_n_operator_in_op_bee_op_chan).alias("n_operator_in_op_bee_op_chan_o_moved_at_day_year_2"),
    )
)

result = (
    metrics
    .groupingSets(
        [["cnt_tier", "moved_at_day_year"], ["cnt_tier"], ["moved_at_day_year"]],
        "cnt_tier", "moved_at_day_year",
    )
    .agg(
        F.sum("n_operator_in_op_bee_op_chan_o").alias("total_n_operator_in_op_bee_op_chan_o"),
        F.count("*").alias("row_count"),
        F.countDistinct("cnt_tier").alias("distinct_cnt_tiers"),
    )
)
