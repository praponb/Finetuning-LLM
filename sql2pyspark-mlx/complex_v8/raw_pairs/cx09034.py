from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id_by_moved = Window.partitionBy("warehouse_id", "sku_id").orderBy("moved_at")
w_warehouse_id_sku_id_by_moved_2 = Window.partitionBy("warehouse_id", "sku_id").orderBy("moved_at", "id")
w_all_by_sku_unit_cost_full = Window.orderBy("sku_unit_cost", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_sku_unit_cos = Window.partitionBy("warehouse_id").orderBy(F.col("sku_unit_cost").desc(), "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_warehouse_id_by_sku_unit_cos_2 = Window.partitionBy("warehouse_id").orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_sku_unit_cost_ties = Window.orderBy("sku_unit_cost")
w_warehouse_id_by_sku_unit_cos_3 = Window.partitionBy("warehouse_id").orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(-1, Window.currentRow)

valid_moves = (
    df_stock_moves
    .filter((F.col("move_type") == "INBOUND") & (F.col("operator").isin("op_ann", "op_chan", "op_dew")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
    )
)

joined_warehouse = (
    valid_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.*",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

ranked = (
    joined_warehouse
    .select(
        "*",
        F.rank().over(w_warehouse_id_sku_id_by_moved).alias("moved_at_rank"),
        F.rank().over(w_warehouse_id_sku_id_by_moved_2).alias("moved_at_rank_2"),
        F.nth_value("moved_at", 2).over(w_all_by_sku_unit_cost_full).alias("second_moved_at"),
        F.lag("moved_at_dayofweek").over(w_warehouse_id_by_sku_unit_cos).alias("prev_moved_at_dayofweek"),
        F.round(F.percent_rank().over(w_warehouse_id_sku_id_by_moved), 3).alias("moved_at_percent_rank"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id), 2).alias("qty_vs_warehouse_id_avg"),
        (F.col("sku_unit_cost") - F.lag("sku_unit_cost").over(w_warehouse_id_by_sku_unit_cos)).alias("sku_unit_cost_change"),
        F.dense_rank().over(w_warehouse_id_sku_id_by_moved_2).alias("moved_at_dense_rank"),
        F.first("moved_at").over(w_warehouse_id_by_sku_unit_cos_2).alias("first_moved_at"),
        F.size(F.collect_set("sku_id").over(w_warehouse_id)).alias("distinct_sku_id_in_warehouse_id"),
        F.round(F.col("moved_at_dayofweek") / F.nullif(F.sum("moved_at_dayofweek").over(w_warehouse_id), F.lit(0)), 4).alias("moved_at_dayofweek_share_of_warehouse_id"),
        F.dense_rank().over(w_all_by_sku_unit_cost_ties).alias("sku_unit_cost_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_sku_unit_cost_ties), 3).alias("sku_unit_cost_percent_rank"),
        F.sum("moved_at_dayofweek").over(w_warehouse_id_by_sku_unit_cos_3).alias("moved_at_dayofweek_sum_2"),
    )
)

top_rows = (
    ranked
    .filter((F.col("moved_at_rank_2") <= 5) & (F.col("moved_at") < F.lit("2025-02-24 08:00:00").cast("timestamp")))
    .drop("moved_at_rank_2")
)

result = (
    top_rows
    .filter(F.col("first_moved_at") < F.lit("2025-02-12 13:00:00").cast("timestamp"))
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "moved_at_dayofweek",
        "warehouse_wh_code",
        "warehouse_city",
        "warehouse_opened_on",
        "sku_unit_cost",
        "moved_at_rank",
        "second_moved_at",
        "prev_moved_at_dayofweek",
        "moved_at_percent_rank",
        "qty_vs_warehouse_id_avg",
        "sku_unit_cost_change",
        "moved_at_dayofweek_share_of_warehouse_id",
        "sku_unit_cost_percent_rank",
        "moved_at_dayofweek_sum_2",
    )
)
