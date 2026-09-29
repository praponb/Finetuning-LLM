from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_city_warehouse_id = Window.partitionBy("warehouse_city", "warehouse_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_city_sku_id_by_uni = Window.partitionBy("warehouse_city", "sku_id").orderBy("unit_price", "id")
w_move_type = Window.partitionBy("move_type")
w_warehouse_city_warehouse_id_2 = Window.partitionBy("warehouse_city", "warehouse_id").orderBy("qty", "id")
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_move_type_sku_id = Window.partitionBy("move_type", "sku_id")
w_warehouse_city_sku_id_by_uni_2 = Window.partitionBy("warehouse_city", "sku_id").orderBy("unit_price")
w_warehouse_city_move_type_by = Window.partitionBy("warehouse_city", "move_type").orderBy("qty", "id")

valid_moves = (
    df_stock_moves
    .filter((~F.col("operator").isin("op_bee", "op_chan")) & (F.col("move_type") != "INBOUND"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

with_details_warehouse = (
    valid_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        "s.move_type_slug",
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

sequenced = (
    with_details_warehouse
    .select(
        "id",
        "warehouse_id",
        "qty",
        "unit_price",
        "move_type_slug",
        "sku_labels",
        "sku_is_active",
        F.nth_value("qty", 2).over(w_warehouse_city_warehouse_id).alias("second_qty"),
        F.ntile(5).over(w_warehouse_city_sku_id_by_uni).alias("unit_price_quintile"),
        F.min("unit_price").over(w_move_type).alias("move_type_min_unit_price"),
        F.ntile(3).over(w_warehouse_city_warehouse_id_2).alias("qty_tercile"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_move_type), F.lit(0)), 4).alias("unit_price_share_of_move_type"),
        F.row_number().over(w_warehouse_city_sku_id_by_uni).alias("unit_price_seq"),
        (F.col("qty") - F.lag("qty").over(w_warehouse_city_warehouse_id_2)).alias("qty_change"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_warehouse_id_sku_id), 2).alias("unit_price_vs_warehouse_id_sku_id_avg"),
        F.max("unit_price").over(w_move_type_sku_id).alias("move_type_sku_id_max_unit_price"),
        F.dense_rank().over(w_warehouse_city_sku_id_by_uni_2).alias("unit_price_dense_rank"),
        F.lead("unit_price", 2, 0).over(w_warehouse_city_move_type_by).alias("next2_unit_price"),
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
    )
)

top_per_group = sequenced.filter((F.col("unit_price_dense_rank") <= 5) & (F.col("warehouse_id") >= 3))

rollup_base = (
    top_per_group
    .groupBy(
        "qty_tercile",
        F.when(F.col("qty_change") >= 150, "A").when(F.col("qty_change") >= -150, "B").otherwise("C").alias("qty_change_tier"),
        "warehouse_id",
    )
    .agg(
        F.round(F.sum("qty_tercile") / F.nullif(F.sum("unit_price"), F.lit(0)), 3).alias("qty_tercile_to_unit_price_ratio"),
        F.round(F.sum("unit_price_quintile") / F.nullif(F.sum("next2_unit_price"), F.lit(0)), 3).alias("unit_price_quintile_to_next2_unit_price_ratio"),
    )
)

result = (
    rollup_base
    .select(
        "qty_tercile",
        "qty_change_tier",
        "qty_tercile_to_unit_price_ratio",
        "unit_price_quintile_to_next2_unit_price_ratio",
        F.abs(F.col("qty_tercile") - 4).alias("qty_tercile_distance"),
        (F.floor(F.col("qty_tercile") / 2) * 2).alias("qty_tercile_bucket"),
    )
    .orderBy(F.col("qty_change_tier").asc_nulls_last())
)
