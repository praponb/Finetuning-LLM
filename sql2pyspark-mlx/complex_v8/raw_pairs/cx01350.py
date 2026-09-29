from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_sku_id_by_qty_ties = Window.partitionBy("sku_id").orderBy("qty")
w_sku_id_by_qty_bucket_running = Window.partitionBy("sku_id").orderBy(F.col("qty_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_sku_id_by_qty_bucket_full = Window.partitionBy("sku_id").orderBy(F.col("qty_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_sku_id_by_qty_bucket = Window.partitionBy("sku_id").orderBy(F.col("qty_bucket").desc(), "id")

recent = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.greatest(F.col("unit_price"), F.col("qty")).alias("greatest_unit_price_qty"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
)

with_trends = (
    recent
    .select(
        "*",
        F.round(F.avg("greatest_unit_price_qty").over(w_sku_id), 2).alias("sku_id_avg_greatest_unit_price_qty"),
        F.max("greatest_unit_price_qty").over(w_sku_id).alias("sku_id_max_greatest_unit_price_qty"),
        F.round(F.cume_dist().over(w_sku_id_by_qty_ties), 3).alias("qty_cume_dist"),
        F.round(F.col("qty_bucket") / F.nullif(F.sum("qty_bucket").over(w_sku_id), F.lit(0)), 4).alias("qty_bucket_share_of_sku_id"),
    )
)

with_parent = (
    with_trends.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select("s.*", F.col("sk.labels").alias("sku_labels"))
)

labeled = (
    with_parent
    .filter(F.col("greatest_unit_price_qty") < 375.2)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "operator",
        "unit_price",
        "greatest_unit_price_qty",
        "qty_bucket",
        "sku_id_max_greatest_unit_price_qty",
        "qty_cume_dist",
        "qty_bucket_share_of_sku_id",
        "sku_labels",
        F.quarter(F.col("moved_at")).alias("moved_at_quarter"),
    )
)

with_windows = (
    labeled
    .select(
        "*",
        F.first("qty_cume_dist", ignorenulls=True).over(w_sku_id_by_qty_bucket_running).alias("first_known_qty_cume_dist"),
        F.count(F.lit(1)).over(w_warehouse_id).alias("rows_in_warehouse_id"),
        F.size(F.collect_set("operator").over(w_warehouse_id)).alias("distinct_operator_in_warehouse_id"),
        F.nth_value("qty_cume_dist", 2).over(w_sku_id_by_qty_bucket_full).alias("second_qty_cume_dist"),
        F.sum("qty_bucket_share_of_sku_id").over(w_warehouse_id).alias("warehouse_id_sum_qty_bucket_share_of_sku_id"),
        F.row_number().over(w_sku_id_by_qty_bucket).alias("seq_no"),
    )
)

result = (
    with_windows
    .select(
        "id",
        F.col("second_qty_cume_dist").alias("m_second_qty_cume_dist"),
        F.col("sku_id_max_greatest_unit_price_qty").alias("sku_id_max_greatest_unit_price_qty_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_second_qty_cume_dist", "sku_id_max_greatest_unit_price_qty_v"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric_name")
)
