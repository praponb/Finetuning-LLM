from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")
w_operator_warehouse_id = Window.partitionBy("operator", "warehouse_id")

recent = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_trends = (
    recent
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "operator",
        "unit_price",
        F.ntile(4).over(w_all_by_unit_price).alias("unit_price_quartile"),
        F.min("unit_price").over(w_operator_warehouse_id).alias("operator_warehouse_id_min_unit_price"),
    )
)

detail_rows_moves = (
    with_trends.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (~F.col("st.move_type").isin("INBOUND", "TRANSFER")), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.move_type",
        "s.unit_price",
        "s.unit_price_quartile",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

combined_details_warehouse = (
    detail_rows_moves.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.capacity") > 4912), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.move_type",
        "s.unit_price_quartile",
        "s.move_id",
        "s.move_moved_at",
        "s.move_qty",
        "s.move_move_type",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
    )
)

result = (
    combined_details_warehouse
    .select(
        "id",
        "move_type",
        "unit_price_quartile",
        "move_id",
        "move_move_type",
        "warehouse_opened_on",
        "warehouse_city",
    )
    .orderBy(F.col("move_id").asc_nulls_last())
)
