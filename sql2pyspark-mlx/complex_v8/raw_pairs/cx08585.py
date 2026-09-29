from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type")
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")

valid = (
    df_stock_moves
    .filter(F.col("moved_at") < F.lit("2025-02-18 15:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.count(F.lit(1)).over(w_move_type).alias("rows_in_move_type"),
        F.ntile(4).over(w_all_by_unit_price).alias("unit_price_quartile"),
        (F.col("unit_price_bucket") - F.lag("unit_price_bucket").over(w_all_by_unit_price)).alias("unit_price_bucket_change"),
        F.round(F.col("unit_price_bucket") / F.nullif(F.sum("unit_price_bucket").over(w_move_type), F.lit(0)), 4).alias("unit_price_bucket_share_of_move_type"),
    )
)

with_details = (
    with_windows.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .filter(F.col("w.opened_on") >= F.lit("2024-11-25").cast("date"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price",
        "s.unit_price_bucket",
        "s.operator_slug",
        "s.rows_in_move_type",
        "s.unit_price_quartile",
        "s.unit_price_bucket_change",
        "s.unit_price_bucket_share_of_move_type",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.category").alias("sku_category"),
    )
)

result = (
    with_details
    .groupBy("warehouse_opened_on")
    .pivot("sku_category", ["Beauty", "Food", "Toys"])
    .agg(F.count("sku_unit_cost"))
)
