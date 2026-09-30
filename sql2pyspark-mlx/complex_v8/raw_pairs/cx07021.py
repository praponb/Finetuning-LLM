from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type = Window.partitionBy("move_type")
w_move_type_sku_id = Window.partitionBy("move_type", "sku_id")
w_warehouse_id_sku_id_by_unit = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("unit_price").asc_nulls_last(), "id")

recent = (
    df_stock_moves
    .filter(F.col("unit_price") < 313.8)
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

ranked = (
    recent
    .withColumn("unit_price_vs_move_type_avg", F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type), 2))
    .withColumn("rows_in_move_type", F.count(F.lit(1)).over(w_move_type))
)

with_trends = (
    ranked
    .select(
        "id",
        "sku_id",
        "unit_price",
        "unit_price_vs_move_type_avg",
        "rows_in_move_type",
        F.count(F.lit(1)).over(w_move_type_sku_id).alias("rows_in_move_type_sku_id"),
        F.lag("unit_price_vs_move_type_avg").over(w_warehouse_id_sku_id_by_unit).alias("prev_unit_price_vs_move_type_avg"),
        F.row_number().over(w_warehouse_id_sku_id_by_unit).alias("unit_price_seq"),
        F.ntile(5).over(w_warehouse_id_sku_id_by_unit).alias("unit_price_quintile"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "sku_id",
        "unit_price",
        "prev_unit_price_vs_move_type_avg",
        "unit_price_seq",
        F.round(F.col("unit_price") / F.nullif(F.col("rows_in_move_type"), F.lit(0)), 3).alias("unit_price_per_rows_in_move_type"),
        (F.col("unit_price_seq") + F.col("rows_in_move_type_sku_id")).alias("unit_price_seq_plus_rows_in_move_type_sku_id"),
    )
    .orderBy(F.col("unit_price").desc())
)
