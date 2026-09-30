from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator")
w_operator_by_cnt_ties = Window.partitionBy("operator").orderBy(F.col("cnt").desc())
w_operator_by_cnt = Window.partitionBy("operator").orderBy(F.col("cnt").desc(), "sku_id", "operator", "move_type_group")
w_operator_by_cnt_running = Window.partitionBy("operator").orderBy(F.col("cnt").desc(), "sku_id", "operator", "move_type_group").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator_by_cnt_full = Window.partitionBy("operator").orderBy(F.col("cnt").desc(), "sku_id", "operator", "move_type_group").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_moves = (
    df_stock_moves
    .filter((F.col("qty") <= 245) & (F.col("unit_price").between(152.7, 323.9)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("move_type").isin("INBOUND", "TRANSFER"), "group_a").when(F.col("move_type") == "ADJUST", "group_b").otherwise("other").alias("move_type_group"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.round(F.col("unit_price") * 0.9, 2).alias("unit_price_adj"),
    )
)

with_parent = (
    valid_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_type_group",
        "s.unit_price_adj",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

with_activity = (
    with_parent.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.moved_at") < F.lit("2025-02-12 13:00:00").cast("timestamp")), "left_anti")
)

grouped = (
    with_activity
    .groupBy("sku_id", "operator", "move_type_group")
    .agg(
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.max("moved_at").alias("last_moved_at"),
        F.count("*").alias("cnt"),
    )
)

metrics = (
    grouped
    .select(
        "sku_id",
        "operator",
        "move_type_group",
        "last_moved_at",
        "cnt",
        F.max("cnt").over(w_operator).alias("operator_max_cnt"),
        F.rank().over(w_operator_by_cnt_ties).alias("cnt_rank"),
        F.lag("last_moved_at").over(w_operator_by_cnt).alias("prev_last_moved_at"),
        F.sum("cnt").over(w_operator_by_cnt_running).alias("running_cnt"),
        F.nth_value("last_moved_at", 2).over(w_operator_by_cnt_full).alias("second_last_moved_at"),
    )
)

result = (
    metrics
    .select(
        "sku_id",
        "operator",
        "cnt_rank",
        "operator_max_cnt",
        "running_cnt",
        "cnt",
    )
    .unpivot(
        ids=["sku_id", "operator"],
        values=["cnt_rank", "operator_max_cnt", "running_cnt", "cnt"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
