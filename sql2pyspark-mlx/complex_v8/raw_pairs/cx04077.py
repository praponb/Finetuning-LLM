from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price_running = Window.orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_all_by_unit_price = Window.orderBy(F.col("unit_price").desc(), "id")

scoped_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.concat(F.col("move_type"), F.lit("#"), F.col("qty").cast("string")).alias("move_type_ref"),
        F.substring(F.trim(F.col("operator")), 1, 4).alias("operator_prefix"),
    )
)

sequenced = (
    scoped_moves
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "unit_price",
        "move_type_ref",
        "operator_prefix",
        F.sum("unit_price").over(w_all_by_unit_price_running).alias("running_unit_price"),
        F.max("unit_price").over(w_warehouse_id).alias("warehouse_id_max_unit_price"),
        F.round(F.percent_rank().over(w_all_by_unit_price), 3).alias("unit_price_percent_rank"),
    )
)

enriched = (
    sequenced.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.move_type",
        "s.moved_at",
        "s.operator_prefix",
        "s.running_unit_price",
        "s.warehouse_id_max_unit_price",
        "s.unit_price_percent_rank",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
    )
)

annotated = (
    enriched
    .withColumn("operator_prefix_move_type_label", F.concat_ws("-", F.col("operator_prefix"), F.col("move_type")))
)

result = (
    annotated
    .select(
        "id",
        "unit_price_percent_rank",
        "running_unit_price",
        "warehouse_id_max_unit_price",
    )
    .unpivot(
        ids=["id"],
        values=["unit_price_percent_rank", "running_unit_price", "warehouse_id_max_unit_price"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric")
)
