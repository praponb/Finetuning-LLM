from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter((F.col("wh_code").isNotNull()) & (F.col("opened_on").isNotNull()))
    .select("capacity", "opened_on")
)

warehouse_side_2 = df_warehouses.select("id", "opened_on")

result = (
    warehouse_side
    .union(warehouse_side_2)
    .orderBy("opened_on")
)
