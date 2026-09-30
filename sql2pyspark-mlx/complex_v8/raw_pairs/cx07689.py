from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_sku_id = Window.partitionBy("move_type", "sku_id")

cleaned_moves = (
    df_stock_moves
    .filter(F.col("operator") == "op_chan")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

windowed = (
    cleaned_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "operator_prefix",
        "move_type_slug",
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_move_type_sku_id), F.lit(0)), 4).alias("unit_price_share_of_move_type_sku_id"),
        F.round(F.avg("qty").over(w_move_type_sku_id), 2).alias("move_type_sku_id_avg_qty"),
    )
)

with_activity = (
    windowed.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.qty") >= 169), "left_semi")
)

result = (
    with_activity
    .groupBy("warehouse_id", "moved_at")
    .pivot("move_type", ["ADJUST", "INBOUND", "OUTBOUND", "TRANSFER"])
    .agg(F.count("unit_price_share_of_move_type_sku_id").alias("count_v"), F.sum("unit_price_share_of_move_type_sku_id").alias("sum_v"))
)
