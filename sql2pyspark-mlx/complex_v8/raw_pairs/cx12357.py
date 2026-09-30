from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_unit_price = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").desc(), "id")
w_warehouse_id_sku_id_by_moved = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("moved_at").desc_nulls_first(), "id")
w_warehouse_id_moved_at_to_dat = Window.partitionBy("warehouse_id", F.to_date("moved_at")).orderBy(F.col("qty").desc(), "id")
w_operator_warehouse_id = Window.partitionBy("operator", "warehouse_id")
w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")
w_operator = Window.partitionBy("operator")
w_sku_id_by_moved_at = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc(), "id")
w_sku_id_by_unit_price_running = Window.partitionBy("sku_id").orderBy(F.col("unit_price").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_unit_price_m = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(-6, Window.currentRow)

valid_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

ranked = (
    valid_moves
    .select(
        "id",
        "sku_id",
        "moved_at",
        "unit_price",
        F.rank().over(w_warehouse_id_by_unit_price).alias("unit_price_rank"),
        F.round(F.percent_rank().over(w_warehouse_id_sku_id_by_moved), 3).alias("moved_at_percent_rank"),
        (F.col("unit_price") - F.lag("unit_price").over(w_warehouse_id_moved_at_to_dat)).alias("unit_price_change"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_operator_warehouse_id), 2).alias("unit_price_vs_operator_warehouse_id_avg"),
        F.round(F.col("qty") - F.avg("qty").over(w_sku_id_warehouse_id), 2).alias("qty_vs_sku_id_warehouse_id_avg"),
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
        F.lead("qty", 3, 0).over(w_sku_id_by_moved_at).alias("next3_qty"),
        F.last("qty", ignorenulls=True).over(w_sku_id_by_unit_price_running).alias("last_known_qty"),
        F.sum("unit_price").over(w_sku_id_by_unit_price_running).alias("running_unit_price"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_operator), F.lit(0)), 4).alias("qty_share_of_operator"),
        F.dense_rank().over(w_warehouse_id_sku_id_by_moved).alias("moved_at_dense_rank"),
        F.round(F.avg("qty").over(w_warehouse_id_by_unit_price_m), 2).alias("qty_avg_7"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_operator), 2).alias("unit_price_vs_operator_avg"),
    )
)

stats = (
    ranked
    .groupBy(
        F.date_trunc("month", "moved_at").alias("moved_at_month"),
        "moved_at_dense_rank",
        "sku_id",
    )
    .agg(
        F.min("running_unit_price").alias("min_running_unit_price"),
        F.min("moved_at").alias("first_moved_at"),
        F.round(F.sum("unit_price_vs_operator_warehouse_id_avg") / F.nullif(F.sum("moved_at_percent_rank"), F.lit(0)), 3).alias("unit_price_vs_operator_warehouse_id_avg_to_moved"),
    )
)

result = (
    stats
    .filter(F.col("unit_price_vs_operator_warehouse_id_avg_to_moved") < 7.5)
    .select(
        "moved_at_month",
        "moved_at_dense_rank",
        "sku_id",
        "min_running_unit_price",
        "unit_price_vs_operator_warehouse_id_avg_to_moved",
    )
    .orderBy("moved_at_dense_rank")
)
