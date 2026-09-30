from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("warehouse_id").orderBy(F.col("qty").desc(), F.col("id").desc())
w_move_type_by_moved_at_prior = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(-6, -1)
w_sku_id_operator = Window.partitionBy("sku_id", "operator")
w_move_type_by_moved_at_moving = Window.partitionBy("move_type").orderBy("moved_at", "id").rowsBetween(-6, Window.currentRow)

valid = df_stock_moves.filter((F.col("operator").isNotNull()) & (F.col("sku_id").isNotNull()))

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

windowed = (
    latest
    .select(
        "*",
        F.round(F.avg("qty").over(w_move_type_by_moved_at_prior), 2).alias("qty_avg_6_prior"),
        F.min("qty").over(w_sku_id_operator).alias("sku_id_operator_min_qty"),
        F.round(F.avg("qty").over(w_move_type_by_moved_at_moving), 2).alias("qty_avg_7"),
        F.min("unit_price").over(w_sku_id_operator).alias("sku_id_operator_min_unit_price"),
    )
)

result = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "qty_avg_6_prior",
        "sku_id_operator_min_qty",
        "sku_id_operator_min_unit_price",
        F.when(F.col("operator").isin("op_ann", "op_eve"), "group_a").when(F.col("operator") == "op_dew", "group_b").otherwise("other").alias("operator_group"),
        (F.floor(F.col("sku_id_operator_min_qty") / 1000) * 1000).alias("sku_id_operator_min_qty_bucket"),
    )
    .orderBy("sku_id_operator_min_qty_bucket", "operator")
)
