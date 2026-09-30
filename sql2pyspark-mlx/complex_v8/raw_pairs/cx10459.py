from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_sku_id = Window.partitionBy("move_sku_id")
w_n_rows_by_min_unit_price = Window.partitionBy("n_rows").orderBy("min_unit_price", "sku_id", "move_sku_id", "move_move_type")
w_last_moved_at_year_distinct = Window.partitionBy(F.year("last_moved_at"), "distinct_sku_id_in_move_sku_id")
w_move_sku_id_by_min_unit_pric = Window.partitionBy("move_sku_id").orderBy(F.col("min_unit_price_share_of_move_sku_id").desc(), "sku_id", "move_sku_id", "move_move_type")
w_move_sku_id_by_min_unit_pric_2 = Window.partitionBy("move_sku_id").orderBy(F.col("min_unit_price_share_of_move_sku_id").desc(), "sku_id", "move_sku_id", "move_move_type").rowsBetween(-6, Window.currentRow)

prepared_moves = df_stock_moves.filter((F.col("operator") != "op_eve") & (F.col("warehouse_id") <= 5))

detail_rows = (
    prepared_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.qty").alias("move_qty"),
    )
)

grouped = (
    detail_rows
    .groupBy("sku_id", "move_sku_id", "move_move_type")
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.max("moved_at").alias("last_moved_at"),
        F.sum(F.when(~F.col("move_move_type").isin("OUTBOUND", "ADJUST"), F.col("unit_price"))).alias("unit_price_move_move_type_not_in_outbou"),
        F.count("*").alias("n_rows"),
        F.round(F.sum("unit_price") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("unit_price_to_qty_ratio"),
        F.round(100 * F.count(F.when(F.col("operator") == "op_chan", 1)) / F.count("*"), 1).alias("pct_operator_op_chan"),
    )
)

windowed = (
    grouped
    .select(
        "sku_id",
        "move_sku_id",
        "move_move_type",
        "min_unit_price",
        "last_moved_at",
        "unit_price_move_move_type_not_in_outbou",
        "n_rows",
        "unit_price_to_qty_ratio",
        "pct_operator_op_chan",
        F.size(F.collect_set("sku_id").over(w_move_sku_id)).alias("distinct_sku_id_in_move_sku_id"),
        F.round(F.col("min_unit_price") / F.nullif(F.sum("min_unit_price").over(w_move_sku_id), F.lit(0)), 4).alias("min_unit_price_share_of_move_sku_id"),
        F.round(F.col("pct_operator_op_chan") / F.nullif(F.sum("pct_operator_op_chan").over(w_move_sku_id), F.lit(0)), 4).alias("pct_operator_op_chan_share_of_move_sku_id"),
        F.lead("unit_price_to_qty_ratio").over(w_n_rows_by_min_unit_price).alias("next_unit_price_to_qty_ratio"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.round(F.col("n_rows") / F.nullif(F.sum("n_rows").over(w_last_moved_at_year_distinct), F.lit(0)), 4).alias("n_rows_share_of_last_moved_at_year_distinct_sku_"),
        F.lag("pct_operator_op_chan", 3).over(w_move_sku_id_by_min_unit_pric).alias("prev3_pct_operator_op_chan"),
        F.sum("n_rows").over(w_move_sku_id_by_min_unit_pric_2).alias("n_rows_sum_7"),
        F.round(F.col("unit_price_to_qty_ratio") / F.nullif(F.sum("unit_price_to_qty_ratio").over(w_last_moved_at_year_distinct), F.lit(0)), 4).alias("unit_price_to_qty_ratio_share_of_last_moved_at_y"),
        F.row_number().over(w_move_sku_id_by_min_unit_pric).alias("min_unit_price_share_of_move_sku_id_seq"),
    )
)

first_rows = (
    sequenced
    .filter((F.col("min_unit_price_share_of_move_sku_id_seq") <= 3) & (F.col("move_move_type") != "ADJUST"))
    .drop("min_unit_price_share_of_move_sku_id_seq")
)

combined_details = (
    first_rows.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.move_sku_id"), "left")
    .select(
        "s.sku_id",
        "s.move_sku_id",
        "s.move_move_type",
        "s.min_unit_price",
        "s.last_moved_at",
        "s.unit_price_move_move_type_not_in_outbou",
        "s.n_rows",
        "s.unit_price_to_qty_ratio",
        "s.pct_operator_op_chan",
        "s.distinct_sku_id_in_move_sku_id",
        "s.next_unit_price_to_qty_ratio",
        "s.prev3_pct_operator_op_chan",
        "s.n_rows_sum_7",
        "s.unit_price_to_qty_ratio_share_of_last_moved_at_y",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

result = (
    combined_details
    .select(
        "sku_id",
        "move_sku_id",
        "last_moved_at",
        "unit_price_move_move_type_not_in_outbou",
        "n_rows",
        "unit_price_to_qty_ratio",
        "pct_operator_op_chan",
        "next_unit_price_to_qty_ratio",
        "prev3_pct_operator_op_chan",
        "n_rows_sum_7",
        "unit_price_to_qty_ratio_share_of_last_moved_at_y",
        "sku_labels",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("last_moved_at")).alias("days_since_last_moved_at"),
        F.substring(F.trim(F.col("move_move_type")), 1, 4).alias("move_move_type_prefix"),
    )
    .orderBy("last_moved_at", F.col("n_rows").desc())
)
