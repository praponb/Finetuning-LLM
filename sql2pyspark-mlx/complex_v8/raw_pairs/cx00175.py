from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("sku_id", "move_type").orderBy("max_unit_price", "id")
w_warehouse_id_by_unit_price_m = Window.partitionBy("warehouse_id").orderBy("unit_price_minus_max", "id")
w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").desc(), "id")
w_warehouse_id = Window.partitionBy("warehouse_id")

base = (
    df_stock_moves
    .filter((F.col("unit_price").between(73.0, 317.0)) & (F.col("qty") <= 349))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("operator").isin("op_dew", "op_chan"), "group_a").when(F.col("operator") == "op_bee", "group_b").otherwise("other").alias("operator_group"),
    )
)

warehouse_id_stats = (
    base
    .groupBy("warehouse_id")
    .agg(F.max("unit_price").alias("max_unit_price"))
)

outliers = (
    base.alias("s")
    .join(warehouse_id_stats.alias("g"), F.col("g.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("s.unit_price") == F.col("g.max_unit_price"))
    .select(
        "s.*",
        "g.max_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.max_unit_price"), 2).alias("unit_price_minus_max"),
    )
)

deduped = (
    outliers
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_trends = (
    deduped
    .select(
        "*",
        F.round(F.percent_rank().over(w_warehouse_id_by_unit_price_m), 3).alias("unit_price_minus_max_percent_rank"),
        F.lag("moved_at").over(w_warehouse_id_by_moved_at).alias("prev_moved_at"),
        F.size(F.collect_set("operator_group").over(w_warehouse_id)).alias("distinct_operator_group_in_warehouse_id"),
        (F.col("unit_price_minus_max") - F.lag("unit_price_minus_max").over(w_warehouse_id_by_unit_price_m)).alias("unit_price_minus_max_change"),
        F.max("max_unit_price").over(w_warehouse_id).alias("warehouse_id_max_max_unit_price"),
        F.size(F.collect_set("operator").over(w_warehouse_id)).alias("distinct_operator_in_warehouse_id"),
    )
)

result = (
    with_trends
    .select("operator_group")
    .distinct()
    .orderBy(F.col("operator_group").asc_nulls_last())
)
