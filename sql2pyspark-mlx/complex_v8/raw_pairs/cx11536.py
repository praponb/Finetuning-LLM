from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id")

base = (
    df_stock_moves
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
    )
)

ranked = (
    base
    .withColumn("distinct_move_type_in_warehouse_id", F.size(F.collect_set("move_type").over(w_warehouse_id)))
    .withColumn("warehouse_id_max_unit_price", F.max("unit_price").over(w_warehouse_id))
)

tagged = (
    ranked
    .withColumn("move_type_operator_prefix_label", F.concat_ws(": ", F.col("move_type"), F.col("operator_prefix")))
)

move_type_stats = (
    tagged
    .groupBy("move_type")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

outliers = (
    tagged.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.unit_price") > F.col("g.avg_unit_price"))
    .select(
        "s.*",
        "g.avg_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.avg_unit_price"), 2).alias("unit_price_minus_avg"),
    )
)

per_group = (
    outliers
    .groupBy("move_type", "warehouse_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.round(F.sum("qty_minus_unit_price") / F.nullif(F.sum("avg_unit_price"), F.lit(0)), 3).alias("qty_minus_unit_price_to_avg_unit_price_ratio"),
    )
    .filter(F.col("row_count") >= 2)
)

result = (
    per_group
    .select(
        "move_type",
        "distinct_ids",
        "row_count",
        "distinct_warehouse_ids",
        "qty_minus_unit_price_to_avg_unit_price_ratio",
        (F.col("row_count") + F.col("distinct_warehouse_ids")).alias("row_count_plus_distinct_warehouse_ids"),
    )
    .orderBy("distinct_warehouse_ids", "row_count_plus_distinct_warehouse_ids")
)
