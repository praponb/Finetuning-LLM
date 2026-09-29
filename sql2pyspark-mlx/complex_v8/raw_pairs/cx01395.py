from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_sku_id = Window.partitionBy("move_type", "sku_id")
w_all_by_unit_price_adj = Window.orderBy(F.col("unit_price_adj").desc(), "id")
w_all_by_unit_price_adj_prior = Window.orderBy(F.col("unit_price_adj").desc(), "id").rowsBetween(-3, -1)
w_all_by_unit_price_adj_ties = Window.orderBy(F.col("unit_price_adj").desc())

cleaned_moves = (
    df_stock_moves
    .filter(F.col("sku_id") >= 10)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.round(F.col("unit_price") * 1.5, 2).alias("unit_price_adj"),
    )
)

ranked = (
    cleaned_moves
    .select(
        "*",
        F.round(F.avg("unit_price_adj").over(w_move_type_sku_id), 2).alias("move_type_sku_id_avg_unit_price_adj"),
        F.ntile(3).over(w_all_by_unit_price_adj).alias("unit_price_adj_tercile"),
        F.round(F.avg("unit_price").over(w_all_by_unit_price_adj_prior), 2).alias("unit_price_avg_3_prior"),
        F.round(F.cume_dist().over(w_all_by_unit_price_adj), 3).alias("unit_price_adj_cume_dist"),
        F.dense_rank().over(w_all_by_unit_price_adj_ties).alias("unit_price_adj_dense_rank"),
    )
)

with_details_warehouse = (
    ranked.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.unit_price",
        "s.unit_price_adj",
        "s.move_type_sku_id_avg_unit_price_adj",
        "s.unit_price_avg_3_prior",
        "s.unit_price_adj_cume_dist",
        "s.unit_price_adj_dense_rank",
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

with_activity = (
    with_details_warehouse.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.operator") == "op_bee") & (F.col("st.qty").between(47, 393)), "left_semi")
    .filter(F.col("unit_price") < 277.6)
)

leaders = with_activity.filter((F.col("unit_price_adj_dense_rank") <= 3) & (F.col("sku_id") >= 14))

result = (
    leaders
    .filter(F.col("unit_price_avg_3_prior") <= 400.2)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "unit_price",
        "unit_price_adj",
        "unit_price_adj_dense_rank",
        "warehouse_capacity",
    )
)
