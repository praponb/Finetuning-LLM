from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_warehouse_id_by_uni = Window.partitionBy("operator", "warehouse_id").orderBy("unit_price", "id")
w_warehouse_id_by_unit_price_p = Window.partitionBy("warehouse_id").orderBy("unit_price_per_qty", "id")
w_operator = Window.partitionBy("operator")
w_sku_id_unit_price_per_qty_se = Window.partitionBy("sku_id", "unit_price_per_qty_seq").orderBy(F.col("unit_price").desc(), "id")
w_sku_id_unit_price_per_qty_se_2 = Window.partitionBy("sku_id", "unit_price_per_qty_seq").orderBy(F.col("unit_price").desc())
w_sku_id_unit_price_tercile = Window.partitionBy("sku_id", "unit_price_tercile")
w_sku_id_unit_price_per_qty_se_3 = Window.partitionBy("sku_id", "unit_price_per_qty_seq").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("unit_price") >= 160.8))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

qualified = (
    recent_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (~F.col("st.operator").isin("op_ann", "op_eve")), "left_semi")
    .filter(F.col("operator") == "op_dew")
)

qualified_2 = (
    qualified.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator").isin("op_chan", "op_dew")), "left_semi")
    .filter(F.col("unit_price") > 160.8)
)

windowed = (
    qualified_2
    .select(
        "*",
        F.ntile(3).over(w_operator_warehouse_id_by_uni).alias("unit_price_tercile"),
        F.row_number().over(w_warehouse_id_by_unit_price_p).alias("unit_price_per_qty_seq"),
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        (F.col("unit_price_tercile") - F.lag("unit_price_tercile").over(w_sku_id_unit_price_per_qty_se)).alias("unit_price_tercile_change"),
        F.rank().over(w_sku_id_unit_price_per_qty_se_2).alias("unit_price_rank"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id_unit_price_tercile)).alias("distinct_warehouse_id_in_sku_id_unit_price_terci"),
        F.last("qty", ignorenulls=True).over(w_sku_id_unit_price_per_qty_se_3).alias("last_known_qty"),
        F.size(F.collect_set("operator").over(w_sku_id_unit_price_tercile)).alias("distinct_operator_in_sku_id_unit_price_tercile"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id_unit_price_tercile), 2).alias("unit_price_vs_sku_id_unit_price_tercile_avg"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("operator") != "op_chan", 1)).alias("move_operator_count"),
        F.count(F.when(F.col("operator") == "op_bee", 1)).alias("move_operator_count_2"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

profiled = (
    sequenced.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        F.coalesce(F.col("st.move_operator_count_2"), F.lit(0)).alias("move_operator_count_2"),
        "st.last_moved_at",
    )
)

result = (
    profiled
    .select(
        "id",
        "unit_price",
        "unit_price_vs_sku_id_unit_price_tercile_avg",
        "unit_price_per_qty",
    )
    .unpivot(
        ids=["id"],
        values=["unit_price", "unit_price_vs_sku_id_unit_price_tercile_avg", "unit_price_per_qty"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
