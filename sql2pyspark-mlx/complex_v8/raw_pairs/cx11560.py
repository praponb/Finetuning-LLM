from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("move_type").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        F.concat_ws(": ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.concat_ws(" / ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label_2"),
    )
)

stats = (
    filtered_moves
    .groupBy("sku_id")
    .agg(
        F.round(100 * F.count(F.when(~F.col("operator").isin("op_eve", "op_chan"), 1)) / F.count("*"), 1).alias("pct_operator_not_in_op_eve_op_ch"),
        F.count(F.when(F.col("operator").isNotNull(), 1)).alias("n_operator_is_not_null"),
    )
)

result = (
    stats
    .select("sku_id", "pct_operator_not_in_op_eve_op_ch")
    .distinct()
    .orderBy(F.col("sku_id").desc_nulls_first())
)
