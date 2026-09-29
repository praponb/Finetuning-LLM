from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator")
w_warehouse_id_sku_id_by_qty = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("qty").desc(), "id")
w_operator_by_qty_ties = Window.partitionBy("operator").orderBy("qty")
w_sku_id_by_qty = Window.partitionBy("sku_id").orderBy(F.col("qty").desc(), "id")
w_sku_id_by_unit_price = Window.partitionBy("sku_id").orderBy("unit_price", "id")

base_moves = (
    df_stock_moves
    .filter(F.col("operator").isin("op_dew", "op_ann", "op_bee"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
    )
)

ranked = (
    base_moves
    .select(
        "*",
        F.size(F.collect_set("warehouse_id").over(w_operator)).alias("distinct_warehouse_id_in_operator"),
        F.ntile(3).over(w_warehouse_id_sku_id_by_qty).alias("qty_tercile"),
        F.min("unit_price").over(w_operator).alias("operator_min_unit_price"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_operator), F.lit(0)), 4).alias("qty_share_of_operator"),
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
        F.dense_rank().over(w_operator_by_qty_ties).alias("qty_dense_rank"),
        F.round(F.percent_rank().over(w_sku_id_by_qty), 3).alias("qty_percent_rank"),
        F.ntile(4).over(w_sku_id_by_unit_price).alias("unit_price_quartile"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_operator), 2).alias("unit_price_vs_operator_avg"),
    )
)

rollup_base = (
    ranked
    .groupBy("unit_price_quartile")
    .agg(
        F.sum("qty").alias("total_qty"),
        F.round(100 * F.count(F.when(F.col("operator") != "op_bee", 1)) / F.count("*"), 1).alias("pct_operator_op_bee"),
        F.min("unit_price").alias("min_unit_price"),
    )
)

result = (
    rollup_base
    .filter(F.col("min_unit_price") < 325.4)
    .select(
        "unit_price_quartile",
        "min_unit_price",
        F.round(F.col("unit_price_quartile") / F.nullif(F.col("min_unit_price"), F.lit(0)), 3).alias("unit_price_quartile_per_min_unit_price"),
    )
)
