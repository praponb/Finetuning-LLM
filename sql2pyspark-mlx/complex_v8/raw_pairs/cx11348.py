from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("qty").desc(), F.col("id").desc())

cleaned = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
    )
)

unique_rows = (
    cleaned
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_details = (
    unique_rows.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

result = (
    with_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "warehouse_city",
        "warehouse_wh_code",
        F.coalesce(F.col("warehouse_city"), F.lit("UNKNOWN")).alias("warehouse_city_or_unknown"),
    )
    .orderBy(F.col("warehouse_city").asc(), "move_type")
)
