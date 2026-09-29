from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("sku_id", "move_type").orderBy("avg_move_qty", "id", "move_id", "move_id_2")

scoped = (
    df_stock_moves
    .filter(F.col("qty") < 256)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.concat_ws(" / ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
)

with_activity = (
    scoped.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.lower(F.col("w.wh_code")).like("%bk%")), "left_semi")
)

line_level = (
    with_activity.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.operator").alias("move_operator"),
    )
)

with_children = (
    line_level.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") < 330.0), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.move_id",
        F.col("st.id").alias("move_id_2"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.move_type").alias("move_move_type_2"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.round(F.avg("qty"), 2).alias("avg_move_qty"),
        F.count(F.when(~F.col("operator").isin("op_eve", "op_ann"), 1)).alias("move_operator_count"),
    )
)

with_stats = (
    with_children.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.avg_move_qty",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

latest = (
    with_stats
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .groupingSets(
        [["move_move_type_2", "move_type"], ["move_move_type_2"], []],
        "move_move_type_2", "move_type",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("move_move_type_2")).alias("move_move_type_2_list"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
    )
    .select(
        F.coalesce(F.col("move_move_type_2"), F.lit("ALL")).alias("move_move_type_2"),
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        "gid",
        "move_move_type_2_list",
        "move_type_list",
    )
)
