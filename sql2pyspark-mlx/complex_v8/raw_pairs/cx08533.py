from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_operator_or_unknown = Window.partitionBy("sku_id", "operator_or_unknown")
w_all_by_unit_price_adj_ties = Window.orderBy("unit_price_adj")
w_warehouse_id_by_moved_at_run = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")

base = (
    df_stock_moves
    .filter(F.col("qty") < 355)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 1.07, 2).alias("unit_price_adj"),
        (F.col("unit_price") + F.col("qty")).alias("unit_price_plus_qty"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

windowed = (
    base
    .select(
        "*",
        F.size(F.collect_set("warehouse_id").over(w_sku_id_operator_or_unknown)).alias("distinct_warehouse_id_in_sku_id_operator_or_unkn"),
        F.dense_rank().over(w_all_by_unit_price_adj_ties).alias("unit_price_adj_dense_rank"),
        F.sum("unit_price").over(w_warehouse_id_by_moved_at_run).alias("running_unit_price"),
        F.round(F.percent_rank().over(w_warehouse_id_by_moved_at), 3).alias("moved_at_percent_rank"),
    )
)

enriched = (
    windowed
    .filter(F.col("running_unit_price") < 4250.1)
    .select(
        "*",
        F.date_trunc("quarter", F.col("moved_at")).alias("moved_at_quarter"),
        F.when(F.col("operator").isin("op_eve", "op_chan"), "group_a").when(F.col("operator") == "op_dew", "group_b").otherwise("other").alias("operator_group"),
    )
)

result = (
    enriched
    .groupBy("moved_at")
    .pivot("operator_or_unknown", ["op_chan", "UNKNOWN", "op_ann", "op_eve"])
    .agg(F.min("distinct_warehouse_id_in_sku_id_operator_or_unkn"))
)
