from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_warehouse_id = Window.partitionBy("sku_id", "warehouse_id")
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_all_by_qty_moving_6 = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(-6, Window.currentRow)
w_qty_seq_by_unit_price_ties = Window.partitionBy("qty_seq").orderBy("unit_price")
w_qty_seq = Window.partitionBy("qty_seq")

prepared_moves = (
    df_stock_moves
    .filter(F.col("unit_price") < 317.0)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("qty").cast("string")).alias("move_type_ref"),
    )
)

ranked = (
    prepared_moves
    .select(
        "id",
        "qty",
        "moved_at",
        "unit_price",
        "operator_clean",
        "move_type_prefix",
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id_warehouse_id), F.lit(0)), 4).alias("unit_price_share_of_sku_id_warehouse_id"),
        F.lead("qty").over(w_all_by_qty).alias("next_qty"),
        F.row_number().over(w_all_by_qty).alias("qty_seq"),
        F.round(F.avg("unit_price").over(w_all_by_qty_moving_6), 2).alias("unit_price_avg_7"),
    )
)

ranked_2 = (
    ranked
    .select(
        "*",
        F.round(F.percent_rank().over(w_qty_seq_by_unit_price_ties), 3).alias("unit_price_percent_rank"),
        F.round(F.col("unit_price_avg_7") - F.avg("unit_price_avg_7").over(w_qty_seq), 2).alias("unit_price_avg_7_vs_qty_seq_avg"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "qty",
        "unit_price",
        "operator_clean",
        "move_type_prefix",
        "unit_price_share_of_sku_id_warehouse_id",
        "qty_seq",
        "unit_price_avg_7",
        "unit_price_percent_rank",
        F.greatest(F.col("unit_price_avg_7"), F.col("qty_seq")).alias("greatest_unit_price_avg_7_qty_seq"),
        F.round(F.col("unit_price_percent_rank") * 1.5, 2).alias("unit_price_percent_rank_adj"),
    )
    .orderBy(
        F.col("unit_price_percent_rank").desc(),
        F.col("unit_price_avg_7").asc_nulls_last(),
        "id",
    )
    .limit(20)
)
