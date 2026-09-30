from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_sku_id_operator_by_unit_pric = Window.partitionBy("sku_id", "operator").orderBy("unit_price", "id")
w_sku_id_operator_by_unit_pric_2 = Window.partitionBy("sku_id", "operator").orderBy("unit_price")

filtered = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
    )
)

without_matches = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
)

ranked = (
    without_matches
    .select(
        "*",
        F.round(F.avg("unit_price_plus_qty").over(w_sku_id_operator), 2).alias("sku_id_operator_avg_unit_price_plus_qty"),
        (F.col("qty") - F.lag("qty").over(w_sku_id_operator_by_unit_pric)).alias("qty_change"),
        F.dense_rank().over(w_sku_id_operator_by_unit_pric_2).alias("unit_price_dense_rank"),
        F.dense_rank().over(w_sku_id_operator_by_unit_pric).alias("unit_price_dense_rank_2"),
    )
)

result = (
    ranked
    .groupBy("unit_price_dense_rank", "warehouse_id")
    .pivot("operator", ["op_dew", "op_chan"])
    .agg(F.min("unit_price_plus_qty"))
    .orderBy("unit_price_dense_rank", "warehouse_id")
)
