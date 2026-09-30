from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at = Window.orderBy(F.col("moved_at").desc(), "id")
w_sku_id_sku_category_by_moved = Window.partitionBy("sku_id", "sku_category").orderBy("moved_at")
w_moved_at_month_by_moved_at = Window.partitionBy(F.date_trunc("month", "moved_at")).orderBy(F.col("moved_at").desc(), "id")
w_warehouse_id = Window.partitionBy("warehouse_id")

prepared = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

with_trends = (
    with_parent
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "sku_category",
        "sku_labels",
        F.lead("moved_at").over(w_all_by_moved_at).alias("next_moved_at"),
        F.round(F.cume_dist().over(w_sku_id_sku_category_by_moved), 3).alias("moved_at_cume_dist"),
    )
)

sequenced = (
    with_trends
    .select(
        "id",
        "sku_category",
        "next_moved_at",
        "moved_at_cume_dist",
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_moved_at_month_by_moved_at))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.round(F.col("moved_at_cume_dist") - F.avg("moved_at_cume_dist").over(w_warehouse_id), 2).alias("moved_at_cume_dist_vs_warehouse_id_avg"),
    )
)

with_activity = (
    sequenced
    .crossJoin(sequenced.agg(F.avg("moved_at_cume_dist_vs_warehouse_id_avg").alias("overall_avg_moved_at_cume_dist_vs_warehouse_id_a")))
    .filter(F.col("moved_at_cume_dist_vs_warehouse_id_avg") > F.col("overall_avg_moved_at_cume_dist_vs_warehouse_id_a"))
    .drop("overall_avg_moved_at_cume_dist_vs_warehouse_id_a")
)

result = (
    with_activity
    .filter(F.col("sku_category").isNotNull())
    .select(
        "id",
        "sku_category",
        "minutes_since_prev_moved_at",
        "moved_at_cume_dist_vs_warehouse_id_avg",
        F.date_trunc("year", F.col("next_moved_at")).alias("next_moved_at_year"),
        F.coalesce(F.col("moved_at_cume_dist"), F.lit(0.0)).alias("moved_at_cume_dist_filled"),
    )
    .orderBy(F.col("moved_at_cume_dist_vs_warehouse_id_avg").asc())
)
