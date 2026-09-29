from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_by_sku_weight_kg_r = Window.partitionBy("move_type").orderBy("sku_weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_qty = Window.partitionBy("sku_id").orderBy("qty", "id")
w_sku_id_warehouse_city_by_sku = Window.partitionBy("sku_id", "warehouse_city").orderBy(F.col("sku_weight_kg").desc(), "id")
w_move_type_by_unit_price_prio = Window.partitionBy("move_type").orderBy(F.col("unit_price").desc(), "id").rowsBetween(-6, -1)
w_move_type_by_sku_weight_kg = Window.partitionBy("move_type").orderBy("sku_weight_kg", "id")
w_move_type_by_unit_price_runn = Window.partitionBy("move_type").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_sku_id_by_qty_full = Window.partitionBy("move_type", "sku_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_warehouse_city_warehouse_id = Window.partitionBy("warehouse_city", "warehouse_id")
w_move_type_sku_id_by_qty = Window.partitionBy("move_type", "sku_id").orderBy("qty", "id")
w_move_type_by_unit_price = Window.partitionBy("move_type").orderBy(F.col("unit_price").desc(), "id")
w_move_type_by_sku_weight_kg_t = Window.partitionBy("move_type").orderBy("sku_weight_kg")

cleaned = (
    df_stock_moves
    .filter(F.month(F.col("moved_at")).isin(2, 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
    )
)

combined_details = (
    cleaned.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .filter(F.col("w.wh_code").isNotNull())
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

windowed = (
    combined_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        "warehouse_wh_code",
        "warehouse_city",
        "sku_labels",
        F.first("sku_weight_kg", ignorenulls=True).over(w_move_type_by_sku_weight_kg_r).alias("first_known_sku_weight_kg"),
        (F.col("unit_price") - F.lag("unit_price").over(w_sku_id_by_qty)).alias("unit_price_change"),
        F.row_number().over(w_sku_id_warehouse_city_by_sku).alias("sku_weight_kg_seq"),
        F.round(F.avg("unit_price").over(w_move_type_by_unit_price_prio), 2).alias("unit_price_avg_6_prior"),
        F.lead("unit_price", 2).over(w_move_type_by_sku_weight_kg).alias("next2_unit_price"),
        F.last("sku_weight_kg", ignorenulls=True).over(w_move_type_by_unit_price_runn).alias("last_known_sku_weight_kg"),
        F.first("qty").over(w_move_type_sku_id_by_qty_full).alias("first_qty"),
        F.first("sku_weight_kg").over(w_move_type_sku_id_by_qty_full).alias("first_sku_weight_kg"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_warehouse_city_warehouse_id), F.lit(0)), 4).alias("qty_share_of_warehouse_city_warehouse_id"),
        F.row_number().over(w_move_type_sku_id_by_qty).alias("qty_seq"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_warehouse_city_warehouse_id), F.lit(0)), 4).alias("unit_price_share_of_warehouse_city_warehouse_id"),
        F.round(F.cume_dist().over(w_move_type_by_unit_price), 3).alias("unit_price_cume_dist"),
        F.rank().over(w_move_type_by_sku_weight_kg_t).alias("sku_weight_kg_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        "warehouse_wh_code",
        "warehouse_city",
        "sku_labels",
        "unit_price_change",
        "sku_weight_kg_seq",
        "unit_price_avg_6_prior",
        "last_known_sku_weight_kg",
        "first_qty",
        "first_sku_weight_kg",
        "rows_in_warehouse_id",
        "qty_share_of_warehouse_city_warehouse_id",
        "qty_seq",
        "unit_price_share_of_warehouse_city_warehouse_id",
        "unit_price_cume_dist",
        F.array_contains(F.col("sku_labels"), "hazmat").alias("has_hazmat"),
    )
    .orderBy(F.col("first_sku_weight_kg").desc(), F.col("unit_price_change").asc())
)
