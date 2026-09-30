from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .select(
        F.col("id").alias("qty"),
        F.col("operator").alias("name"),
        F.col("warehouse_id").alias("ref_id"),
        F.lit("move").alias("src"),
    )
)

warehouse_side = (
    df_warehouses
    .filter(F.col("wh_code").isNotNull())
    .select(
        "capacity",
        "city",
        "id",
        F.lit("warehouse").alias("src"),
    )
)

result = (
    move_side
    .union(warehouse_side)
    .orderBy("ref_id")
)
