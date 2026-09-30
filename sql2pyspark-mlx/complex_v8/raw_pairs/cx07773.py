from pyspark.sql import functions as F

recent = df_warehouses

warehouse_rows = (
    recent
    .filter((F.lower(F.col("wh_code")).like("%x1%")) & (F.year(F.col("opened_on")) == 2025))
    .select("capacity", "opened_on", F.lit("warehouse").alias("source"))
)

warehouse_side = (
    df_warehouses
    .filter((F.col("capacity") <= 11506) & (F.col("city") == "Bangkok"))
    .select("capacity", "opened_on", F.lit("warehouse_2").alias("source"))
)

result = (
    warehouse_rows
    .union(warehouse_side)
    .orderBy("source")
)
