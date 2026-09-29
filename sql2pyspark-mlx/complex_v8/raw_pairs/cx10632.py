from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_move_type_by_qty_prior_1 = Window.partitionBy("move_type").orderBy(F.col("qty").desc(), "id").rowsBetween(-1, -1)
w_sku_id_move_type_by_unit_pri = Window.partitionBy("sku_id", "move_type").orderBy(F.col("unit_price").desc())
w_move_type_by_qty_full = Window.partitionBy("move_type").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_qty = Window.partitionBy("move_type").orderBy(F.col("qty").desc(), "id")

base_moves = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.concat(F.col("operator"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("operator_ref"),
    )
)

sequenced = (
    base_moves
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "operator",
        F.count(F.lit(1)).over(w_warehouse_id_sku_id).alias("rows_in_warehouse_id_sku_id"),
        F.sum("qty").over(w_move_type_by_qty_prior_1).alias("qty_sum_1_prior"),
        F.round(F.percent_rank().over(w_sku_id_move_type_by_unit_pri), 3).alias("unit_price_percent_rank"),
        F.first("qty").over(w_move_type_by_qty_full).alias("first_qty"),
        (F.col("qty") - F.lag("qty").over(w_move_type_by_qty)).alias("qty_change"),
    )
)

derived = (
    sequenced
    .select(
        "id",
        "move_type",
        "operator",
        "rows_in_warehouse_id_sku_id",
        "qty_sum_1_prior",
        "first_qty",
        "qty_change",
        F.abs(F.col("qty_change") - -200).alias("qty_change_distance"),
        F.concat_ws("-", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

result = (
    derived
    .groupBy("operator")
    .pivot("move_type", ["INBOUND", "TRANSFER", "OUTBOUND", "ADJUST"])
    .agg(F.min("qty_sum_1_prior"))
)
