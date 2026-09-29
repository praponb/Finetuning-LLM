from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("sku_id").orderBy("move_type_len", "id")
w_dedup_2 = Window.partitionBy("warehouse_id").orderBy(F.col("unit_price").desc(), F.col("id").desc())

base_moves = (
    df_stock_moves
    .filter(F.col("qty") <= 299)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
    )
)

deduped = (
    base_moves
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

unique_rows = (
    deduped
    .withColumn("recency_rank", F.row_number().over(w_dedup_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

enriched = (
    unique_rows
    .select(
        "*",
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label_2"),
        F.round(F.col("unit_price") / F.nullif(F.col("move_type_len"), F.lit(0)), 3).alias("unit_price_per_move_type_len"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

sku_id_stats = (
    enriched
    .groupBy("sku_id")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

outliers = (
    enriched.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.unit_price") > F.col("g.avg_unit_price"))
    .select(
        "s.*",
        "g.avg_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.avg_unit_price"), 2).alias("unit_price_minus_avg"),
    )
)

result = (
    outliers
    .rollup("warehouse_id", "move_type_len", "operator_or_unknown")
    .agg(
        F.min("unit_price_minus_avg").alias("min_unit_price_minus_avg"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
    )
    .select(
        "warehouse_id",
        "move_type_len",
        F.coalesce(F.col("operator_or_unknown"), F.lit("ALL")).alias("operator_or_unknown"),
        "min_unit_price_minus_avg",
        "operator_list",
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("move_type_len").asc_nulls_last(),
        F.col("operator_or_unknown").asc_nulls_last(),
    )
)
