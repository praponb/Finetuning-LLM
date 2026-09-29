from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_operator_by_sku = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("sku_unit_cost").desc())
w_sku_id = Window.partitionBy("sku_id")
w_warehouse_id_operator_by_sku_2 = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(-3, Window.currentRow)
w_warehouse_id_operator_by_sku_3 = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_operator_by_sku_4 = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("sku_unit_cost").desc(), "id").rowsBetween(-6, -1)

prepared = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
    )
)

with_details_sku = (
    prepared.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.qty_plus_unit_price",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

windowed = (
    with_details_sku
    .select(
        "id",
        "sku_id",
        "qty",
        F.round(F.cume_dist().over(w_warehouse_id_operator_by_sku), 3).alias("sku_unit_cost_cume_dist"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.max("qty_plus_unit_price").over(w_warehouse_id_operator_by_sku_2).alias("qty_plus_unit_price_max_4"),
        F.nth_value("qty_plus_unit_price", 2).over(w_warehouse_id_operator_by_sku_3).alias("second_qty_plus_unit_price"),
        F.round(F.avg("sku_unit_cost").over(w_warehouse_id_operator_by_sku_4), 2).alias("sku_unit_cost_avg_6_prior"),
        F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_sku_id), F.lit(0)), 4).alias("qty_share_of_sku_id"),
    )
)

labeled = (
    windowed
    .filter(F.col("sku_unit_cost_avg_6_prior") < 220.4)
    .select(
        "*",
        F.coalesce(F.col("rows_in_sku_id"), F.lit(0)).alias("rows_in_sku_id_filled"),
        (F.col("sku_unit_cost_cume_dist") + F.col("qty")).alias("sku_unit_cost_cume_dist_plus_qty"),
        F.when(F.col("rows_in_sku_id") >= 13, "HIGH").when(F.col("rows_in_sku_id") >= 6, "MEDIUM").otherwise("LOW").alias("rows_in_sku_id_tier"),
    )
)

result = (
    labeled
    .groupBy("sku_id")
    .pivot("rows_in_sku_id_tier", ["HIGH", "LOW"])
    .agg(F.sum("rows_in_sku_id"))
)
