from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_category = Window.partitionBy("sku_category")
w_all_by_qty = Window.orderBy(F.col("qty").desc_nulls_first(), "id")
w_all_by_qty_ties = Window.orderBy(F.col("qty").desc_nulls_first())
w_all_by_qty_moving_2 = Window.orderBy(F.col("qty").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_moved_at_month_by_qty = Window.partitionBy(F.date_trunc("month", "moved_at")).orderBy("qty", "id")

scoped = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
    )
)

with_parent_sku = (
    scoped.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("sk.category") != "Electronics")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        F.col("sk.category").alias("sku_category"),
    )
)

sequenced = (
    with_parent_sku
    .select(
        "*",
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_sku_category), F.lit(0)), 4).alias("qty_share_of_sku_category"),
        F.rank().over(w_all_by_qty).alias("qty_rank"),
        F.round(F.percent_rank().over(w_all_by_qty_ties), 3).alias("qty_percent_rank"),
        F.round(F.col("qty") - F.avg("qty").over(w_sku_category), 2).alias("qty_vs_sku_category_avg"),
        F.lag("moved_at").over(w_all_by_qty).alias("prev_moved_at"),
        F.sum("qty").over(w_all_by_qty_moving_2).alias("qty_sum_3"),
        F.rank().over(w_moved_at_month_by_qty).alias("qty_rank_2"),
    )
)

top_per_group = (
    sequenced
    .filter((F.col("qty_rank") <= 2) & (F.col("moved_at") < F.lit("2025-02-24 11:00:00").cast("timestamp")))
)

result = (
    top_per_group
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "sku_category",
        "qty_share_of_sku_category",
        "qty_rank",
        "qty_percent_rank",
        "qty_vs_sku_category_avg",
        "qty_sum_3",
        "qty_rank_2",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.concat(F.col("operator"), F.lit("#"), F.col("id").cast("string")).alias("operator_ref"),
    )
    .orderBy(F.col("sku_id").desc())
)
