from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_all_by_least_qty_unit_price = Window.orderBy(F.col("least_qty_unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stock_moves
    .filter((F.col("qty").between(99, 458)) | (F.col("moved_at") < F.lit("2025-03-08 11:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("unit_price") >= 313.8, "A").when(F.col("unit_price") >= 136.0, "B").otherwise("C").alias("unit_price_tier"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("id").cast("string")).alias("move_type_ref"),
    )
)

derived = (
    filtered
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        "unit_price_tier",
        "move_type_ref",
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
    )
)

windowed = (
    derived
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price_tier",
        "move_type_ref",
        F.sum("qty").over(w_warehouse_id_sku_id).alias("warehouse_id_sku_id_sum_qty"),
        F.size(F.collect_set("operator").over(w_warehouse_id_sku_id)).alias("distinct_operator_in_warehouse_id_sku_id"),
        F.max("qty").over(w_all_by_least_qty_unit_price).alias("max_qty_so_far"),
    )
)

without_matches = (
    windowed.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.to_date(F.col("st.moved_at")) < F.lit("2025-03-02").cast("date")), "left_anti")
)

annotated = (
    without_matches
    .filter(F.col("max_qty_so_far") <= 3250)
    .withColumn("greatest_qty_max_qty_so_far", F.greatest(F.col("qty"), F.col("max_qty_so_far")))
)

result = (
    annotated
    .select(
        "id",
        F.col("distinct_operator_in_warehouse_id_sku_id").alias("distinct_operator_in_warehouse_id_sku_id_v"),
        F.col("qty").alias("qty_v"),
        F.col("max_qty_so_far").alias("m_max_qty_so_far"),
        F.col("greatest_qty_max_qty_so_far").alias("m_greatest_qty_max_qty_so_far"),
    )
    .unpivot(
        ids=["id"],
        values=["distinct_operator_in_warehouse_id_sku_id_v", "qty_v", "m_max_qty_so_far", "m_greatest_qty_max_qty_so_far"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
)
