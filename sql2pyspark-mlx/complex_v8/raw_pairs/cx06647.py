from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city = Window.partitionBy("city")
w_city_by_capacity_moving_3 = Window.partitionBy("city").orderBy(F.col("capacity").desc(), "id").rowsBetween(-3, Window.currentRow)
w_city_by_capacity_ties = Window.partitionBy("city").orderBy(F.col("capacity").desc())
w_move_id_move_sku_id_by_move = Window.partitionBy("move_id", "move_sku_id").orderBy(F.col("move_unit_price_2").asc_nulls_last())
w_move_id_move_sku_id_by_move_2 = Window.partitionBy("move_id", "move_sku_id").orderBy(F.col("move_unit_price_2").asc_nulls_last(), "id", "move_id", "move_id_2")
w_move_id = Window.partitionBy("move_id")

valid = (
    df_warehouses
    .filter(F.col("city").isin("Kuala Lumpur", "Singapore"))
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
    )
)

windowed = (
    valid
    .select(
        "id",
        "capacity",
        "opened_on",
        F.round(F.avg("capacity").over(w_city_by_capacity_moving_3), 2).alias("capacity_avg_4"),
        F.min("capacity").over(w_city).alias("city_min_capacity"),
        F.dense_rank().over(w_city_by_capacity_ties).alias("capacity_dense_rank"),
    )
)

detail_rows_moves = (
    windowed.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.opened_on",
        "s.capacity_avg_4",
        "s.capacity_dense_rank",
        F.col("st.id").alias("move_id"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

line_level = (
    detail_rows_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.opened_on",
        "s.capacity_avg_4",
        "s.move_id",
        "s.move_sku_id",
        "s.move_unit_price",
        F.col("st.id").alias("move_id_2"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.unit_price").alias("move_unit_price_2"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

enriched = (
    line_level.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.move_sku_id"), "inner")
    .filter(F.col("sk.weight_kg") > 12.4)
    .select(
        "s.id",
        "s.opened_on",
        "s.move_id",
        "s.move_sku_id",
        "s.move_unit_price",
        "s.move_id_2",
        "s.move_qty",
        "s.move_unit_price_2",
        "s.move_moved_at",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.rank().over(w_move_id_move_sku_id_by_move).alias("move_unit_price_2_rank"),
        (F.col("move_unit_price_2") - F.lag("move_unit_price_2").over(w_move_id_move_sku_id_by_move_2)).alias("move_unit_price_2_change"),
        F.ntile(4).over(w_move_id_move_sku_id_by_move_2).alias("move_unit_price_2_quartile"),
        F.round(F.col("move_unit_price_2") / F.nullif(F.sum("move_unit_price_2").over(w_move_id), F.lit(0)), 4).alias("move_unit_price_2_share_of_move_id"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "opened_on",
        "move_id",
        "move_id_2",
        "move_qty",
        "move_unit_price_2",
        "move_moved_at",
        "sku_weight_kg",
        "move_unit_price_2_rank",
        "move_unit_price_2_change",
        F.greatest(F.col("sku_weight_kg"), F.col("move_qty")).alias("greatest_sku_weight_kg_move_qty"),
    )
    .orderBy("greatest_sku_weight_kg_move_qty", F.col("move_unit_price_2").desc())
)
