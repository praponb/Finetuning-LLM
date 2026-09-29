from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_qty_full = Window.partitionBy("operator").orderBy(F.col("qty").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_warehouse_id_sku_id = Window.partitionBy("warehouse_id", "sku_id")
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_sku_id_warehouse_id_by_unit = Window.partitionBy("sku_id", "warehouse_id").orderBy("unit_price", "id").rowsBetween(-2, -1)
w_sku_id_warehouse_id_by_unit_2 = Window.partitionBy("sku_id", "warehouse_id").orderBy("unit_price", "id")
w_operator_by_unit_price_full = Window.partitionBy("operator").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_warehouse_id_by_unit_3 = Window.partitionBy("sku_id", "warehouse_id").orderBy("unit_price")

valid = (
    df_stock_moves
    .filter((F.col("operator").isNotNull()) & (F.col("sku_id") <= 9))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
    )
)

metrics = (
    valid
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "unit_price",
        F.first("qty").over(w_operator_by_qty_full).alias("first_qty"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.count(F.lit(1)).over(w_warehouse_id_sku_id).alias("rows_in_warehouse_id_sku_id"),
        F.min("qty").over(w_warehouse_id).alias("warehouse_id_min_qty"),
        F.lead("qty").over(w_all_by_unit_price).alias("next_qty"),
        F.max("unit_price").over(w_sku_id_warehouse_id_by_unit).alias("unit_price_max_2_prior"),
        F.lag("qty", 3).over(w_sku_id_warehouse_id_by_unit_2).alias("prev3_qty"),
        F.last("unit_price").over(w_operator_by_unit_price_full).alias("last_unit_price"),
        F.round(F.percent_rank().over(w_sku_id_warehouse_id_by_unit_3), 3).alias("unit_price_percent_rank"),
        F.rank().over(w_sku_id_warehouse_id_by_unit_2).alias("unit_price_rank"),
        F.ntile(5).over(w_all_by_unit_price).alias("unit_price_quintile"),
    )
)

result = (
    metrics
    .select(
        "id",
        "first_qty",
        "rows_in_warehouse_id_sku_id",
        "warehouse_id_min_qty",
        "prev3_qty",
        "unit_price_percent_rank",
        "unit_price_rank",
        "unit_price_quintile",
        F.round(F.col("warehouse_id_min_qty") / F.nullif(F.col("first_qty"), F.lit(0)), 3).alias("warehouse_id_min_qty_per_first_qty"),
        (F.col("rows_in_warehouse_id") + F.col("first_qty")).alias("rows_in_warehouse_id_plus_first_qty"),
    )
    .orderBy(
        F.col("prev3_qty").asc_nulls_last(),
        "warehouse_id_min_qty_per_first_qty",
    )
)
