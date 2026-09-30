from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter((F.upper(F.trim(F.col("wh_code"))).like("B%")) & (F.col("opened_on") < F.lit("2025-07-03").cast("date")))
    .select("capacity")
)

move_rows = df_stock_moves.select("qty")

result = (
    warehouse_side
    .subtract(move_rows)
    .orderBy("capacity")
)
