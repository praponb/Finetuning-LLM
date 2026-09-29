from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("sku_id", "move_type").orderBy(F.col("qty").desc(), F.col("id").desc())

prepared = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

deduped = (
    prepared
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

derived = (
    deduped
    .select(
        "*",
        F.abs(F.col("qty") - 169).alias("qty_distance"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

result = (
    derived
    .filter(F.col("sku_id") >= 13)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price_filled",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("unit_price_filled").asc_nulls_last(),
    )
)
