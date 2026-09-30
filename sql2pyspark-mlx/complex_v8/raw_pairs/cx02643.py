from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_group_by_qty = Window.partitionBy("operator_group").orderBy("qty", "id")
w_all_by_qty_ties = Window.orderBy(F.col("qty").desc())
w_sku_id = Window.partitionBy("sku_id")

base = (
    df_stock_moves
    .filter((F.col("unit_price") > 189.4) & (F.hour(F.col("moved_at")).between(6, 8)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.when(F.col("operator").isin("op_dew", "op_ann"), "group_a").when(F.col("operator") == "op_chan", "group_b").otherwise("other").alias("operator_group"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.min("moved_at").alias("first_moved_at"))
)

with_stats = (
    base.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.first_moved_at")
)

with_windows = (
    with_stats
    .select(
        "*",
        F.row_number().over(w_operator_group_by_qty).alias("rn"),
        F.rank().over(w_all_by_qty_ties).alias("qty_rank"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_id), 2).alias("unit_price_vs_sku_id_avg"),
    )
)

result = (
    with_windows
    .rollup("rn", "operator_group")
    .agg(F.min("qty_rank").alias("min_qty_rank"))
    .orderBy(F.col("rn").asc_nulls_last(), F.col("operator_group").asc_nulls_last())
)
