from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter(F.col("operator") != "op_ann")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
    )
)

move_side = (
    filtered
    .filter((F.col("sku_id") <= 19) & (F.col("move_type").isNotNull()))
    .select(
        "id",
        F.col("move_type").alias("name"),
        F.lit("move").alias("record_type"),
    )
)

warehouse_side = (
    df_warehouses
    .filter((F.col("wh_code").isNotNull()) & (F.col("city") == "Kuala Lumpur"))
    .select(
        "id",
        F.col("wh_code").alias("name"),
        F.lit("warehouse").alias("record_type"),
    )
)

unioned = move_side.unionByName(warehouse_side)

result = (
    unioned
    .groupBy("record_type")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("record_type")
)
