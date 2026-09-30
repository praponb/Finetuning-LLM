from pyspark.sql import functions as F

cleaned_warehouses = (
    df_warehouses
    .filter((F.col("capacity") < 11768) | (F.col("wh_code").isNotNull()))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.upper(F.trim(F.col("wh_code"))).alias("wh_code_clean"),
    )
)

warehouse_rows = (
    cleaned_warehouses
    .filter(F.length(F.trim(F.col("wh_code"))) >= 8)
    .select("capacity", F.col("wh_code").alias("name"))
)

move_side = df_stock_moves.select("warehouse_id", "move_type")

warehouse_rows_2 = (
    df_warehouses
    .filter((F.col("capacity") <= 11005) & (F.col("city") != "Singapore"))
    .select("id", "wh_code")
)

result = (
    warehouse_rows
    .union(move_side)
    .union(warehouse_rows_2)
)
