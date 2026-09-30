from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_warehouse_id_by_unit = Window.partitionBy("sku_id", "warehouse_id").orderBy("unit_price_filled", "id")
w_move_type = Window.partitionBy("move_type")
w_sku_id_warehouse_id_by_unit_2 = Window.partitionBy("sku_id", "warehouse_id").orderBy("unit_price_filled")
w_sku_id_warehouse_id_by_unit_3 = Window.partitionBy("sku_id", "warehouse_id").orderBy("unit_price_filled", "id").rowsBetween(-1, Window.currentRow)

prepared_moves = (
    df_stock_moves
    .filter(F.col("move_type").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
)

sequenced = (
    prepared_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "moved_at_week",
        "days_since_moved_at",
        "unit_price_filled",
        F.lead("qty").over(w_sku_id_warehouse_id_by_unit).alias("next_qty"),
        F.round(F.avg("unit_price_filled").over(w_move_type), 2).alias("move_type_avg_unit_price_filled"),
        F.round(F.percent_rank().over(w_sku_id_warehouse_id_by_unit_2), 3).alias("unit_price_filled_percent_rank"),
        F.rank().over(w_sku_id_warehouse_id_by_unit).alias("unit_price_filled_rank"),
        F.sum("unit_price").over(w_sku_id_warehouse_id_by_unit_3).alias("unit_price_sum_2"),
    )
)

with_details_sku = (
    sequenced.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select("s.*", F.col("sk.labels").alias("sku_labels"))
)

labeled = (
    with_details_sku
    .select(
        "*",
        F.round(F.col("unit_price_sum_2") * 1.1, 2).alias("unit_price_sum_2_adj"),
        (F.col("days_since_moved_at") - F.col("unit_price_filled")).alias("days_since_moved_at_minus_unit_price_filled"),
    )
)

result = (
    labeled
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        "moved_at_week",
        "days_since_moved_at",
        "unit_price_filled",
        "move_type_avg_unit_price_filled",
        "unit_price_filled_percent_rank",
        "unit_price_sum_2",
        "sku_labels",
        "unit_price_sum_2_adj",
        "days_since_moved_at_minus_unit_price_filled",
        F.date_format(F.col("moved_at_week"), "yyyy-MM").alias("moved_at_week_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at_2"),
    )
    .orderBy("unit_price_filled")
)
