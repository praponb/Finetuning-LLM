from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator")
w_sku_id_by_moved_at_month = Window.partitionBy("sku_id").orderBy("moved_at_month", "id")
w_moved_at_month_moved_at_mont = Window.partitionBy("moved_at_month", F.month("moved_at")).orderBy(F.col("qty").desc(), "id")

prepared = (
    df_stock_moves
    .filter((F.col("move_type") != "TRANSFER") & (F.col("operator").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.month(F.col("moved_at")).alias("moved_at_month"),
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
    )
)

sequenced = (
    prepared
    .select(
        "*",
        F.sum("qty").over(w_operator).alias("operator_sum_qty"),
        F.lag("unit_price").over(w_sku_id_by_moved_at_month).alias("prev_unit_price"),
        F.lag("moved_at_month", 1, 0).over(w_moved_at_month_moved_at_mont).alias("prev_moved_at_month"),
    )
)

tagged = (
    sequenced
    .filter(F.col("sku_id") <= 14)
    .select(
        "*",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
        F.round(F.col("least_qty_unit_price") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("least_qty_unit_price_per_unit_price"),
    )
)

result = (
    tagged
    .rollup("operator", "warehouse_id", "move_type")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(100 * F.count(F.when(F.col("move_type").isin("INBOUND", "TRANSFER"), 1)) / F.count("*"), 1).alias("pct_move_type_in_inbound_transfe"),
    )
    .orderBy(
        F.col("operator").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
    )
)
