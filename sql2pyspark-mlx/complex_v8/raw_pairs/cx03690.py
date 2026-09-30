from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_operator = Window.partitionBy("warehouse_id", "operator")
w_move_type_or_unknown_by_qty = Window.partitionBy("move_type_or_unknown").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_or_unknown_by_qty_2 = Window.partitionBy("move_type_or_unknown").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_stock_moves
    .filter((F.col("sku_id") <= 11) & (F.col("qty") > 78))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "unit_price",
        "unit_price_bucket",
        "operator_clean",
        F.round(F.avg("qty").over(w_warehouse_id_operator), 2).alias("warehouse_id_operator_avg_qty"),
        F.max("unit_price_bucket").over(w_move_type_or_unknown_by_qty).alias("max_unit_price_bucket_so_far"),
        F.sum("unit_price").over(w_warehouse_id_operator).alias("warehouse_id_operator_sum_unit_price"),
        F.nth_value("unit_price_bucket", 2).over(w_move_type_or_unknown_by_qty_2).alias("second_unit_price_bucket"),
        F.min("qty").over(w_warehouse_id_operator).alias("warehouse_id_operator_min_qty"),
    )
)

combined_details = (
    sequenced.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.unit_price",
        "s.unit_price_bucket",
        "s.operator_clean",
        "s.max_unit_price_bucket_so_far",
        "s.warehouse_id_operator_min_qty",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "unit_price",
        "max_unit_price_bucket_so_far",
        "warehouse_id_operator_min_qty",
        "warehouse_wh_code",
        "warehouse_opened_on",
    )
    .orderBy(F.col("unit_price").desc_nulls_first())
)
