from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_sku_id_by_moved_at = Window.partitionBy("move_type", "sku_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_operator_group_operator_by_u = Window.partitionBy("operator_group", "operator").orderBy("unit_price", "id")
w_operator_group_operator_by_u_2 = Window.partitionBy("operator_group", "operator").orderBy("unit_price")
w_moved_at_to_date_warehouse_i = Window.partitionBy(F.to_date("moved_at"), "warehouse_id")
w_sku_id_by_unit_price = Window.partitionBy("sku_id").orderBy(F.col("unit_price").desc(), "id")

prepared = (
    df_stock_moves
    .filter(F.col("unit_price") <= 396.4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("operator").isin("op_ann", "op_bee"), "group_a").when(F.col("operator") == "op_eve", "group_b").otherwise("other").alias("operator_group"),
    )
)

with_trends = (
    prepared
    .select(
        "*",
        F.first("moved_at").over(w_move_type_sku_id_by_moved_at).alias("first_moved_at"),
        F.ntile(3).over(w_operator_group_operator_by_u).alias("unit_price_tercile"),
        F.round(F.percent_rank().over(w_operator_group_operator_by_u_2), 3).alias("unit_price_percent_rank"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_moved_at_to_date_warehouse_i), 2).alias("unit_price_vs_moved_at_to_date_warehouse_id_avg"),
        F.min("unit_price").over(w_moved_at_to_date_warehouse_i).alias("moved_at_to_date_warehouse_id_min_unit_price"),
        (F.col("qty") - F.lag("qty").over(w_sku_id_by_unit_price)).alias("qty_change"),
    )
)

sku_id_stats = (
    with_trends
    .groupBy("sku_id")
    .agg(F.avg("unit_price_vs_moved_at_to_date_warehouse_id_avg").alias("avg_unit_price_vs_moved_at_to_date_warehouse_id_"))
)

vs_group = (
    with_trends.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.unit_price_vs_moved_at_to_date_warehouse_id_avg") > F.col("g.avg_unit_price_vs_moved_at_to_date_warehouse_id_"))
    .select(
        "s.*",
        "g.avg_unit_price_vs_moved_at_to_date_warehouse_id_",
        F.round(F.col("s.unit_price_vs_moved_at_to_date_warehouse_id_avg") - F.col("g.avg_unit_price_vs_moved_at_to_date_warehouse_id_"), 2).alias("unit_price_vs_moved_at_to_date_warehouse_id_avg_"),
    )
)

result = (
    vs_group
    .select("id", "qty", "qty_change")
    .unpivot(
        ids=["id"],
        values=["qty", "qty_change"],
        variableColumnName="metric",
        valueColumnName="value",
    )
)
