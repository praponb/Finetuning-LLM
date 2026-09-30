from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")

base = (
    df_stock_moves
    .filter((F.col("unit_price").between(58.6, 416.0)) & (F.col("move_type").isin("ADJUST", "OUTBOUND")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.abs(F.col("qty") - 314).alias("qty_distance"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

with_windows = (
    base
    .withColumn("unit_price_share_of_warehouse_id", F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_warehouse_id), F.lit(0)), 4))
)

screened = (
    with_windows.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type") == "OUTBOUND"), "left_semi")
)

qualified = (
    screened.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.qty") <= 349) & (F.col("st.moved_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp")), "left_anti")
)

result = (
    qualified
    .groupingSets(
        [["warehouse_id", "operator", "sku_id"], ["warehouse_id"], []],
        "warehouse_id", "operator", "sku_id",
    )
    .agg(
        F.round(F.stddev("unit_price_share_of_warehouse_id"), 2).alias("stddev_unit_price_share_of_warehouse_id"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("operator").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
