from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_unit_price_ties = Window.partitionBy("operator").orderBy("unit_price")
w_operator_by_unit_price = Window.partitionBy("operator").orderBy("unit_price", "id")

prepared_moves = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

windowed = (
    prepared_moves
    .select(
        "*",
        F.rank().over(w_operator_by_unit_price_ties).alias("unit_price_rank"),
        (F.col("days_since_moved_at") - F.lag("days_since_moved_at").over(w_operator_by_unit_price)).alias("days_since_moved_at_change"),
    )
)

result = (
    windowed
    .groupingSets(
        [["warehouse_id", "unit_price_rank"], ["warehouse_id"], []],
        "warehouse_id", "unit_price_rank",
    )
    .agg(
        F.grouping("warehouse_id").alias("is_total_warehouse_id"),
        F.round(F.sum("least_qty_unit_price") / F.nullif(F.sum("days_since_moved_at_change"), F.lit(0)), 3).alias("least_qty_unit_price_to_days_since_moved_at_chan"),
        F.round(F.sum("unit_price") / F.nullif(F.sum("days_since_moved_at_change"), F.lit(0)), 3).alias("unit_price_to_days_since_moved_at_change_ratio"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("unit_price_rank").asc_nulls_last(),
    )
)
