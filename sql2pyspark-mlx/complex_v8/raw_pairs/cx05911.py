from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.concat_ws(" / ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

move_rows = recent.select("qty", F.col("operator").alias("category"))

warehouse_rows = (
    df_warehouses
    .filter((F.col("opened_on").isNotNull()) & (F.col("city") == "Kuala Lumpur"))
    .select(F.col("capacity").alias("qty"), F.col("wh_code").alias("category"))
)

result = (
    move_rows
    .unionByName(warehouse_rows)
    .distinct()
    .orderBy("category", "qty")
)
