from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_move_operator_in_op_bee_op = Window.partitionBy("n_move_operator_in_op_bee_op_a")
w_n_move_operator_in_op_bee_op_2 = Window.partitionBy("n_move_operator_in_op_bee_op_a", "move_operator").orderBy("total_move_unit_price", "move_id", "move_operator", "sku_id")
w_n_move_operator_in_op_bee_op_3 = Window.partitionBy("n_move_operator_in_op_bee_op_a", "move_operator").orderBy("total_move_unit_price", "move_id", "move_operator", "sku_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_moves = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("moved_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.when(F.col("operator").isin("op_ann", "op_bee"), "group_a").when(F.col("operator") == "op_eve", "group_b").otherwise("other").alias("operator_group"),
    )
)

enriched = (
    valid_moves
    .select(
        "*",
        F.concat(F.col("operator"), F.lit("#"), F.col("sku_id").cast("string")).alias("operator_ref"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

detail_rows = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator_group",
        "s.operator_ref",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

without_matches = (
    detail_rows.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type") == "INBOUND"), "left_semi")
    .filter(F.col("move_operator") == "op_bee")
)

per_group = (
    without_matches
    .filter(F.col("move_id") <= 66)
    .groupBy("move_id", "move_operator", "sku_id")
    .agg(
        F.countDistinct("move_id").alias("distinct_move_ids"),
        F.sum("move_unit_price").alias("total_move_unit_price"),
        F.sum(F.when(F.col("move_operator").isin("op_bee", "op_ann"), 1).otherwise(0)).alias("n_move_operator_in_op_bee_op_a"),
        F.sum(F.when(F.col("move_operator").isin("op_dew", "op_ann"), F.col("qty"))).alias("qty_move_operator_in_op_dew_op_a"),
    )
    .filter(F.col("total_move_unit_price") > 86.3)
)

metrics = (
    per_group
    .select(
        "move_id",
        "move_operator",
        "sku_id",
        "distinct_move_ids",
        "total_move_unit_price",
        "n_move_operator_in_op_bee_op_a",
        F.round(F.avg("total_move_unit_price").over(w_n_move_operator_in_op_bee_op), 2).alias("n_move_operator_in_op_bee_op_a_avg_total_move_un"),
        F.lead("total_move_unit_price", 2, 0).over(w_n_move_operator_in_op_bee_op_2).alias("next2_total_move_unit_price"),
        F.first("total_move_unit_price", ignorenulls=True).over(w_n_move_operator_in_op_bee_op_3).alias("first_known_total_move_unit_price"),
        F.round(F.col("distinct_move_ids") - F.avg("distinct_move_ids").over(w_n_move_operator_in_op_bee_op), 2).alias("distinct_move_ids_vs_n_move_operator_in_op_bee_o"),
        F.lead("total_move_unit_price", 3).over(w_n_move_operator_in_op_bee_op_2).alias("next3_total_move_unit_price"),
    )
)

with_details = (
    metrics.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("st.sku_id"), "left")
    .select(
        "s.*",
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

result = (
    with_details
    .select(
        "move_id",
        "move_operator",
        "sku_id",
        "distinct_move_ids",
        "total_move_unit_price",
        "n_move_operator_in_op_bee_op_a",
        "n_move_operator_in_op_bee_op_a_avg_total_move_un",
        "next2_total_move_unit_price",
        "first_known_total_move_unit_price",
        "distinct_move_ids_vs_n_move_operator_in_op_bee_o",
        "next3_total_move_unit_price",
        "move_warehouse_id",
        "move_sku_id",
        "move_qty",
        "sku_unit_cost",
        F.substring(F.trim(F.col("move_operator")), 1, 4).alias("move_operator_prefix"),
    )
)
