from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("sku_id").orderBy(F.col("qty").desc(), F.col("id").desc())
w_sku_id_by_qty = Window.partitionBy("sku_id").orderBy("qty", "id")
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")

prepared = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

latest = (
    prepared
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

sequenced = (
    latest
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        (F.col("qty") - F.lag("qty").over(w_sku_id_by_qty)).alias("qty_change"),
        F.min("qty").over(w_warehouse_id_sku_id).alias("warehouse_id_sku_id_min_qty"),
    )
)

joined = (
    sequenced.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("w.opened_on") >= F.lit("2024-11-25").cast("date"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price",
        "s.warehouse_id_sku_id_min_qty",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

joined_warehouse = (
    joined.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.unit_price",
        "s.warehouse_id_sku_id_min_qty",
        "s.warehouse_city",
        "s.warehouse_opened_on",
        F.col("w.opened_on").alias("warehouse_opened_on_2"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

result = (
    joined_warehouse
    .select(
        "id",
        "sku_id",
        "warehouse_id_sku_id_min_qty",
        "warehouse_city",
        "warehouse_opened_on",
        "warehouse_capacity",
        F.date_trunc("month", F.col("warehouse_opened_on")).alias("warehouse_opened_on_month"),
        (F.floor(F.col("warehouse_capacity") / 5000) * 5000).alias("warehouse_capacity_bucket"),
    )
    .orderBy(F.col("warehouse_opened_on").desc(), F.col("id").desc())
)
