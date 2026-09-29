from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("sku_id", "operator").orderBy("qty", "id")

prepared = (
    df_stock_moves
    .filter(F.col("sku_id") >= 5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled_2"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)

latest_per_key = (
    prepared
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .groupingSets(
        [["warehouse_id", "move_type"], ["warehouse_id"]],
        "warehouse_id", "move_type",
    )
    .agg(F.sum("qty_filled_2").alias("total_qty_filled_2"))
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
    )
)
