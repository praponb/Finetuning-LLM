from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at_full = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_qty_tier = Window.partitionBy("sku_id", "qty_tier")
w_qty_tier_by_unit_price_runni = Window.partitionBy("qty_tier").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_or_unknown_by_move = Window.partitionBy("move_type_or_unknown").orderBy(F.col("moved_at").desc(), "id").rowsBetween(-1, -1)
w_sku_id_by_unit_price = Window.partitionBy("sku_id").orderBy("unit_price", "id")
w_sku_id_by_unit_price_ties = Window.partitionBy("sku_id").orderBy("unit_price")
w_qty_tier_by_unit_price = Window.partitionBy("qty_tier").orderBy("unit_price", "id")
w_qty_tier_move_type_or_unknow = Window.partitionBy("qty_tier", "move_type_or_unknown")
w_qty_tier_by_qty_avg_1_prior = Window.partitionBy("qty_tier").orderBy("qty_avg_1_prior", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_qty_tier_by_qty_avg_1_prior_2 = Window.partitionBy("qty_tier").orderBy("qty_avg_1_prior", "id")

prepared = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) | (~F.col("operator").isin("op_ann", "op_eve")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.when(F.col("qty") >= 394, "large").when(F.col("qty") >= 189, "medium").otherwise("small").alias("qty_tier"),
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
)

with_trends = (
    prepared
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        "qty_tier",
        "move_type_or_unknown",
        F.last("unit_price").over(w_all_by_moved_at_full).alias("last_unit_price"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id_qty_tier), F.lit(0)), 4).alias("unit_price_share_of_sku_id_qty_tier"),
        F.count(F.lit(1)).over(w_sku_id_qty_tier).alias("rows_in_sku_id_qty_tier"),
        F.sum("unit_price").over(w_qty_tier_by_unit_price_runni).alias("running_unit_price"),
        F.round(F.avg("qty").over(w_move_type_or_unknown_by_move), 2).alias("qty_avg_1_prior"),
        F.row_number().over(w_sku_id_by_unit_price).alias("unit_price_seq"),
        F.round(F.cume_dist().over(w_sku_id_by_unit_price_ties), 3).alias("unit_price_cume_dist"),
        (F.col("unit_price") - F.lag("unit_price").over(w_qty_tier_by_unit_price)).alias("unit_price_change"),
    )
)

top_rows = (
    with_trends
    .filter(F.col("unit_price_seq") == 1)
    .drop("unit_price_seq")
)

ranked = (
    top_rows
    .select(
        "*",
        F.size(F.collect_set("sku_id").over(w_qty_tier_move_type_or_unknow)).alias("distinct_sku_id_in_qty_tier_move_type_or_unknown"),
        F.round(F.col("running_unit_price") - F.avg("running_unit_price").over(w_qty_tier_move_type_or_unknow), 2).alias("running_unit_price_vs_qty_tier_move_type_or_unkn"),
        F.last("qty").over(w_qty_tier_by_qty_avg_1_prior).alias("last_qty"),
        F.round(F.percent_rank().over(w_qty_tier_by_qty_avg_1_prior_2), 3).alias("qty_avg_1_prior_percent_rank"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_qty_tier_move_type_or_unknow), F.lit(0)), 4).alias("qty_share_of_qty_tier_move_type_or_unknown"),
    )
)

result = (
    ranked
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "qty",
        "moved_at",
        "unit_price",
        "qty_tier",
        "last_unit_price",
        "unit_price_share_of_sku_id_qty_tier",
        "rows_in_sku_id_qty_tier",
        "running_unit_price",
        "qty_avg_1_prior",
        "unit_price_cume_dist",
        "unit_price_change",
        "last_qty",
        "qty_avg_1_prior_percent_rank",
        "qty_share_of_qty_tier_move_type_or_unknown",
    )
    .orderBy(F.col("moved_at").desc(), F.col("id").desc())
)
