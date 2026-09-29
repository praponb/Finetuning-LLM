from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_by_move_type_len_prio = Window.partitionBy("sku_id").orderBy(F.col("move_type_len").desc(), "id").rowsBetween(-3, -1)
w_sku_id = Window.partitionBy("sku_id")
w_latest = Window.partitionBy("move_type").orderBy("move_type_len_share_of_sku_id", "id")
w_dedup = Window.partitionBy("operator", "sku_id").orderBy("moved_at", "id")

prepared_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
)

metrics = (
    prepared_moves
    .select(
        "*",
        F.round(F.avg("move_type_len").over(w_sku_id_by_move_type_len_prio), 2).alias("move_type_len_avg_3_prior"),
        F.round(F.col("move_type_len") / F.nullif(F.sum("move_type_len").over(w_sku_id), F.lit(0)), 4).alias("move_type_len_share_of_sku_id"),
        F.round(F.avg("move_type_len").over(w_sku_id), 2).alias("sku_id_avg_move_type_len"),
    )
)

latest_per_key = (
    metrics
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

unique_rows = (
    latest_per_key
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

grouped = (
    unique_rows
    .groupBy("operator")
    .agg(
        F.percentile_approx("move_type_len_share_of_sku_id", 0.5).alias("median_move_type_len_share_of_sku_id"),
        F.round(F.avg("sku_id_avg_move_type_len"), 2).alias("avg_sku_id_avg_move_type_len"),
        F.sum(F.when(F.col("move_type").isin("INBOUND", "ADJUST"), 1).otherwise(0)).alias("n_move_type_in_inbound_adjust"),
        F.avg("move_type_len_share_of_sku_id").alias("avg_move_type_len_share_of_sku_id_check"),
    )
    .filter(F.col("avg_move_type_len_share_of_sku_id_check") > 0.2)
    .drop("avg_move_type_len_share_of_sku_id_check")
)

result = (
    grouped
    .groupingSets(
        [["n_move_type_in_inbound_adjust", "operator"], ["n_move_type_in_inbound_adjust"], []],
        "n_move_type_in_inbound_adjust", "operator",
    )
    .agg(
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.sum(F.when(F.col("operator").isin("op_eve", "op_bee", "op_chan"), 1).otherwise(0)).alias("n_operator_in_op_eve_op_bee_op"),
    )
    .orderBy(
        F.col("n_move_type_in_inbound_adjust").asc_nulls_last(),
        F.col("operator").asc_nulls_last(),
    )
)
