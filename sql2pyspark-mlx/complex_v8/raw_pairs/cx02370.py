from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_id_move_type_by_unit_pri = Window.partitionBy("sku_id", "move_type").orderBy(F.col("unit_price").desc(), "id")
w_sku_id_operator_by_unit_pric = Window.partitionBy("sku_id", "operator").orderBy("unit_price", "id").rowsBetween(-2, -1)
w_sku_id_operator_by_unit_pric_2 = Window.partitionBy("sku_id", "operator").orderBy("unit_price", "id")
w_sku_id_move_type_by_unit_pri_2 = Window.partitionBy("sku_id", "move_type").orderBy(F.col("unit_price").desc())
w_sku_id_operator_by_unit_pric_3 = Window.partitionBy("sku_id", "operator").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_stock_moves
    .filter(F.col("operator").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.abs(F.col("qty") - 168).alias("qty_distance"),
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.sum("qty_distance").over(w_warehouse_id).alias("warehouse_id_sum_qty_distance"),
        F.rank().over(w_sku_id_move_type_by_unit_pri).alias("unit_price_rank"),
        F.max("qty").over(w_sku_id_operator_by_unit_pric).alias("qty_max_2_prior"),
        F.row_number().over(w_sku_id_move_type_by_unit_pri).alias("rn"),
        F.lag("qty", 2).over(w_sku_id_operator_by_unit_pric_2).alias("prev2_qty"),
        F.lead("unit_price").over(w_sku_id_move_type_by_unit_pri).alias("next_unit_price"),
        F.ntile(5).over(w_sku_id_operator_by_unit_pric_2).alias("unit_price_quintile"),
        F.size(F.collect_set("move_type").over(w_warehouse_id)).alias("distinct_move_type_in_warehouse_id"),
        F.round(F.cume_dist().over(w_sku_id_move_type_by_unit_pri_2), 3).alias("unit_price_cume_dist"),
        F.first("qty_distance").over(w_sku_id_operator_by_unit_pric_3).alias("first_qty_distance"),
        F.row_number().over(w_sku_id_operator_by_unit_pric_2).alias("seq_no"),
    )
)

result = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "qty_distance",
        "warehouse_id_sum_qty_distance",
        "unit_price_rank",
        "qty_max_2_prior",
        "rn",
        "prev2_qty",
        "next_unit_price",
        "first_qty_distance",
        "seq_no",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)
