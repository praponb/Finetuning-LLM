from pyspark.sql import functions as F

scoped_warehouses = (
    df_warehouses
    .filter((F.col("capacity") <= 11506) & (F.length(F.trim(F.col("wh_code"))) >= 7))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.length(F.trim(F.col("wh_code"))).alias("wh_code_len"),
    )
)

warehouse_side = (
    scoped_warehouses
    .filter(F.upper(F.trim(F.col("wh_code"))).like("C%"))
    .select(F.col("wh_code_len").alias("key_id"))
)

warehouse_side_2 = (
    df_warehouses
    .filter(F.col("capacity") <= 11506)
    .select(F.col("id").alias("key_id"))
)

result = warehouse_side.intersect(warehouse_side_2)
