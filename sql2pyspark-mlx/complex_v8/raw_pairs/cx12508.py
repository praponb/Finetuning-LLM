from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator")
w_warehouse_city_operator_by_q = Window.partitionBy("warehouse_city", "operator").orderBy(F.col("qty").desc())

cleaned_moves = (
    df_stock_moves
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

joined = (
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.size(F.col("sk.labels")) >= 1)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("sk.labels").alias("sku_labels"),
    )
)

combined_details = (
    joined.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

windowed = (
    combined_details
    .withColumn("operator_max_warehouse_capacity", F.max("warehouse_capacity").over(w_operator))
    .withColumn("qty_cume_dist", F.round(F.cume_dist().over(w_warehouse_city_operator_by_q), 3))
)

result = (
    windowed
    .filter(F.col("qty") <= 349)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "sku_labels",
        "operator_max_warehouse_capacity",
        "qty_cume_dist",
    )
    .orderBy(F.col("warehouse_id").desc(), F.col("sku_id").desc())
)
