from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_operator_by_qty_runni = Window.partitionBy("sku_id", "operator").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_qty_moving_6 = Window.orderBy("qty", "id").rowsBetween(-6, Window.currentRow)
w_sku_id_operator_by_qty = Window.partitionBy("sku_id", "operator").orderBy(F.col("qty").desc(), "id")
w_all_by_qty_ties = Window.orderBy("qty")
w_sku_id_operator = Window.partitionBy("sku_id", "operator")

scoped = (
    df_stock_moves
    .filter(F.col("sku_id") >= 15)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)

metrics = (
    scoped
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        "qty_minus_unit_price",
        "operator_slug",
        F.last("unit_price", ignorenulls=True).over(w_sku_id_operator_by_qty_runni).alias("last_known_unit_price"),
        F.max("qty_minus_unit_price").over(w_all_by_qty_moving_6).alias("qty_minus_unit_price_max_7"),
        F.ntile(4).over(w_sku_id_operator_by_qty).alias("qty_quartile"),
        F.rank().over(w_all_by_qty_ties).alias("qty_rank"),
        F.size(F.collect_set("warehouse_id").over(w_sku_id_operator)).alias("distinct_warehouse_id_in_sku_id_operator"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_sku_id_operator), F.lit(0)), 4).alias("unit_price_share_of_sku_id_operator"),
    )
)

with_parent_sku = (
    metrics.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.unit_cost") < 320.4), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.unit_price",
        "s.qty_minus_unit_price_max_7",
        "s.qty_quartile",
        "s.qty_rank",
        "s.unit_price_share_of_sku_id_operator",
        F.col("sk.labels").alias("sku_labels"),
    )
)

result = (
    with_parent_sku
    .rollup("warehouse_id", "qty_rank", "sku_id")
    .agg(F.sum("qty_quartile").alias("total_qty_quartile"))
)
