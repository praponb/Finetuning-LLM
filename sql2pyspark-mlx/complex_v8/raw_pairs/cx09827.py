from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("sku_id").orderBy(F.col("qty_per_unit_price").desc(), F.col("id").desc())

scoped = (
    df_stock_moves
    .filter(F.col("sku_id") <= 13)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
)

unique_rows = (
    scoped
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_parent_sku = (
    unique_rows.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        "s.qty_per_unit_price",
        F.col("sk.labels").alias("sku_labels"),
        F.col("w.city").alias("warehouse_city"),
    )
)

result = (
    with_parent_sku
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "unit_price",
        "qty_per_unit_price",
        "sku_labels",
        "warehouse_city",
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("id").cast("string")).alias("move_type_ref"),
    )
    .orderBy(F.col("sku_id").desc(), "move_type_ref", "id")
    .limit(10)
)
