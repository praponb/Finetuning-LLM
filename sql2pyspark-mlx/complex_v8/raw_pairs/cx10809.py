from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter((F.col("city") == "Kuala Lumpur") & (F.col("capacity") < 11005))
    .select("id", F.col("wh_code").alias("category"))
)

warehouse_side_2 = (
    df_warehouses
    .filter((~F.col("city").isin("Kuala Lumpur", "Singapore")) & (F.length(F.trim(F.col("wh_code"))) >= 5))
    .select("id", "wh_code")
)

result = (
    warehouse_side
    .intersectAll(warehouse_side_2)
    .orderBy(F.col("category").desc(), "id")
)
