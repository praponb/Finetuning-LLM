from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_id_moved_at_to_date_by_a = Window.partitionBy("sku_id", F.to_date("moved_at")).orderBy("avg_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_moved_at_to_date_by_a_2 = Window.partitionBy("sku_id", F.to_date("moved_at")).orderBy("avg_qty", "id")
w_all_by_moved_at_ties = Window.orderBy("moved_at")
w_all_by_qty_minus_avg = Window.orderBy(F.col("qty_minus_avg").desc(), "id")
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_moved_at_to_date_warehouse_i = Window.partitionBy(F.to_date("moved_at"), "warehouse_id")

scoped = (
    df_stock_moves
    .filter((F.col("unit_price") <= 323.9) | (F.col("operator") == "op_eve"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
    )
)

sku_id_stats = (
    scoped
    .groupBy("sku_id")
    .agg(F.avg("qty").alias("avg_qty"))
)

vs_group = (
    scoped.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.qty") > F.col("g.avg_qty"))
    .select(
        "s.*",
        "g.avg_qty",
        F.round(F.col("s.qty") - F.col("g.avg_qty"), 2).alias("qty_minus_avg"),
    )
)

windowed = (
    vs_group
    .select(
        "*",
        F.max("qty").over(w_warehouse_id).alias("warehouse_id_max_qty"),
        F.round(F.col("avg_qty") / F.nullif(F.sum("avg_qty").over(w_warehouse_id), F.lit(0)), 4).alias("avg_qty_share_of_warehouse_id"),
        F.first("avg_qty", ignorenulls=True).over(w_sku_id_moved_at_to_date_by_a).alias("first_known_avg_qty"),
        F.lead("moved_at", 2).over(w_sku_id_moved_at_to_date_by_a_2).alias("next2_moved_at"),
    )
)

ranked = (
    windowed
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_moved_at_ties), 3).alias("moved_at_percent_rank"),
        F.lag("unit_price_plus_qty").over(w_all_by_qty_minus_avg).alias("prev_unit_price_plus_qty"),
        F.lag("qty_minus_avg").over(w_all_by_moved_at).alias("prev_qty_minus_avg"),
        F.round(F.col("warehouse_id_max_qty") / F.nullif(F.sum("warehouse_id_max_qty").over(w_moved_at_to_date_warehouse_i), F.lit(0)), 4).alias("warehouse_id_max_qty_share_of_moved_at_to_date_w"),
    )
)

annotated = (
    ranked
    .select(
        "*",
        F.coalesce(F.col("warehouse_id_max_qty"), F.lit(0)).alias("warehouse_id_max_qty_filled"),
    )
)

totals = (
    annotated
    .filter(F.hour(F.col("next2_moved_at")).between(8, 12))
    .groupBy("warehouse_id", "sku_id")
    .agg(
        F.sum(F.when(F.col("move_type") == "INBOUND", 1).otherwise(0)).alias("n_move_type_inbound"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.max("qty").alias("max_qty_check"),
    )
    .filter(F.col("max_qty_check") > 154)
    .drop("max_qty_check")
)

result = (
    totals
    .select("warehouse_id", "sku_id")
    .orderBy(F.col("sku_id").desc_nulls_first())
)
