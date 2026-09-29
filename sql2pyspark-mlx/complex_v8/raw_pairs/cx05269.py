from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("warehouse_id").orderBy(F.col("qty").desc(), F.col("id").desc())
w_latest = Window.partitionBy("move_type").orderBy("qty", "id")

filtered_moves = (
    df_stock_moves
    .filter((F.col("move_type").isin("ADJUST", "TRANSFER")) | (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd").alias("moved_at_label"),
    )
)

deduped = (
    filtered_moves
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_details_warehouse = (
    deduped.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
    )
)

with_details = (
    with_details_warehouse.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.moved_at_label",
        "s.warehouse_city",
        F.col("w.city").alias("warehouse_city_2"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

latest = (
    with_details
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "moved_at_label",
        "warehouse_city",
        "warehouse_city_2",
        "warehouse_capacity",
        F.least(F.col("qty"), F.col("warehouse_capacity")).alias("least_qty_warehouse_capacity"),
    )
    .orderBy(
        F.col("sku_id").desc_nulls_first(),
        F.col("moved_at_label").asc_nulls_last(),
    )
)
