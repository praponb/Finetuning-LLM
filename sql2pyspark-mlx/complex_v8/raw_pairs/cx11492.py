from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_warehouse_id = Window.partitionBy("move_type", "warehouse_id")
w_all_by_qty_per_unit_price = Window.orderBy(F.col("qty_per_unit_price").desc(), "id")
w_all_by_unit_price_bucket = Window.orderBy("unit_price_bucket", "id")
w_operator_by_unit_price_bucke = Window.partitionBy("operator").orderBy("unit_price_bucket", "id")
w_operator_by_unit_price_bucke_2 = Window.partitionBy("operator").orderBy("unit_price_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_operator = Window.partitionBy("operator")

base = (
    df_stock_moves
    .filter(F.col("unit_price") >= 152.7)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

windowed = (
    base
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "unit_price_bucket",
        F.size(F.collect_set("sku_id").over(w_move_type_warehouse_id)).alias("distinct_sku_id_in_move_type_warehouse_id"),
        F.lead("unit_price", 3).over(w_all_by_qty_per_unit_price).alias("next3_unit_price"),
        F.count(F.lit(1)).over(w_move_type_warehouse_id).alias("rows_in_move_type_warehouse_id"),
        (F.col("qty_per_unit_price") - F.lag("qty_per_unit_price").over(w_all_by_unit_price_bucket)).alias("qty_per_unit_price_change"),
        F.dense_rank().over(w_all_by_unit_price_bucket).alias("unit_price_bucket_dense_rank"),
        F.row_number().over(w_operator_by_unit_price_bucke).alias("rn"),
        F.last("qty_per_unit_price", ignorenulls=True).over(w_operator_by_unit_price_bucke_2).alias("last_known_qty_per_unit_price"),
        F.round(F.col("qty") - F.avg("qty").over(w_operator), 2).alias("qty_vs_operator_avg"),
        F.ntile(3).over(w_all_by_unit_price_bucket).alias("unit_price_bucket_tercile"),
        F.round(F.col("unit_price_bucket") - F.avg("unit_price_bucket").over(w_move_type_warehouse_id), 2).alias("unit_price_bucket_vs_move_type_warehouse_id_avg"),
    )
)

top_rows = windowed.filter(F.col("rn") <= 3)

result = (
    top_rows
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price_bucket",
        "distinct_sku_id_in_move_type_warehouse_id",
        "rows_in_move_type_warehouse_id",
        "qty_per_unit_price_change",
        "rn",
        "last_known_qty_per_unit_price",
        "qty_vs_operator_avg",
        "unit_price_bucket_tercile",
        F.round(F.col("qty") / F.nullif(F.col("qty_vs_operator_avg"), F.lit(0)), 3).alias("qty_per_qty_vs_operator_avg"),
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
    )
    .orderBy(F.col("qty_per_qty_vs_operator_avg").desc())
)
