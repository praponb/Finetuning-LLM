from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_category_warehouse_id = Window.partitionBy("sku_category", "warehouse_id")
w_operator_by_qty_ties = Window.partitionBy("operator").orderBy("qty")

cleaned = (
    df_stock_moves
    .filter(F.col("qty").between(61, 370))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

combined_details = (
    cleaned.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_type_slug",
        "s.move_type_or_unknown",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("sk.category").alias("sku_category"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_sku_category_warehouse_id), 2).alias("unit_price_vs_sku_category_warehouse_id_avg"),
        F.round(F.percent_rank().over(w_operator_by_qty_ties), 3).alias("qty_percent_rank"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_sku_category_warehouse_id), F.lit(0)), 4).alias("qty_share_of_sku_category_warehouse_id"),
    )
)

result = (
    windowed
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "move_type_slug",
        "warehouse_city",
        "warehouse_wh_code",
        "warehouse_capacity",
        "unit_price_vs_sku_category_warehouse_id_avg",
        "qty_percent_rank",
    )
    .orderBy(F.col("id").desc(), "operator")
)
