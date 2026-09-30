from pyspark.sql import functions as F

valid_warehouses = (
    df_warehouses
    .filter(F.col("city").isin("Singapore", "Bangkok"))
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
    )
)

warehouse_side = (
    df_warehouses
    .select(
        F.col("wh_code").alias("category"),
        F.col("city").alias("category_2"),
        F.lit("warehouse").alias("record_type"),
    )
)

move_side = (
    df_stock_moves
    .filter((F.col("move_type") == "OUTBOUND") & (F.col("operator") == "op_dew"))
    .select("move_type", "operator", F.lit("move").alias("record_type"))
)

result = (
    warehouse_side
    .union(move_side)
    .distinct()
    .orderBy("category_2")
)
