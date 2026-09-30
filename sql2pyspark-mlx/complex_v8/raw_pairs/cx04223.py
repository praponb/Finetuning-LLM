from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("operator").orderBy("unit_price", "id")

recent_moves = (
    df_stock_moves
    .filter(F.col("moved_at") < F.lit("2025-02-24 12:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws(": ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("id").cast("string")).alias("move_type_ref"),
        F.concat_ws("|", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

deduped = (
    recent_moves
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_children = (
    deduped.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.move_type_ref",
        "s.operator_move_type_label",
        F.col("st.id").alias("move_id"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        "st.move_type",
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.qty").alias("move_qty"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("move_count"),
        F.max("unit_price").alias("max_move_unit_price"),
    )
)

with_counts = (
    with_children.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.max_move_unit_price",
    )
)

result = (
    with_counts
    .groupingSets(
        [["operator", "move_id", "move_warehouse_id"], ["operator"], ["move_id"]],
        "operator", "move_id", "move_warehouse_id",
    )
    .agg(
        F.grouping("operator").alias("is_total_operator"),
        F.max("move_moved_at").alias("last_move_moved_at"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
    )
    .select(
        F.coalesce(F.col("operator"), F.lit("ALL")).alias("operator"),
        "move_id",
        "move_warehouse_id",
        "is_total_operator",
        "last_move_moved_at",
        "cnt",
        "operator_list",
    )
    .orderBy(
        F.col("operator").asc_nulls_last(),
        F.col("move_id").asc_nulls_last(),
        F.col("move_warehouse_id").asc_nulls_last(),
    )
)
