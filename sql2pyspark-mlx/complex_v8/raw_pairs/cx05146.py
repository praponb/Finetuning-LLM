from pyspark.sql import functions as F

prepared = (
    df_warehouses
    .filter(F.col("wh_code").isNotNull())
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
    )
)

warehouse_rows = prepared.select("capacity")

sku_side = df_skus.select(F.col("id").alias("capacity"))

result = (
    warehouse_rows
    .intersectAll(sku_side)
    .orderBy("capacity")
)
