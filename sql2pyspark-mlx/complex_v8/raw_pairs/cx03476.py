from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .select(
        F.col("capacity").alias("ref_id"),
        "opened_on",
        F.col("id").alias("key_id"),
        F.lit("warehouse").alias("record_type"),
    )
)

warehouse_rows_2 = (
    df_warehouses
    .filter((F.lower(F.col("wh_code")).like("%x1%")) & (F.col("capacity") < 11506))
    .select(
        F.col("capacity").alias("ref_id"),
        "opened_on",
        F.col("id").alias("key_id"),
        F.lit("warehouse_2").alias("record_type"),
    )
)

result = (
    warehouse_rows
    .unionByName(warehouse_rows_2)
    .orderBy("ref_id", "key_id")
)
