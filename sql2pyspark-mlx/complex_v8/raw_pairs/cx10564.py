from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("move_type").orderBy(F.col("operator_len").desc(), F.col("id").desc())

base = (
    df_stock_moves
    .filter(F.col("sku_id") >= 2)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
    )
)

sku_id_stats = (
    base
    .groupBy("sku_id")
    .agg(F.avg("qty").alias("avg_qty"))
)

compared = (
    base.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.qty") > F.col("g.avg_qty"))
    .select(
        "s.*",
        "g.avg_qty",
        F.round(F.col("s.qty") - F.col("g.avg_qty"), 2).alias("qty_minus_avg"),
    )
)

latest_per_key = (
    compared
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        "move_type_or_unknown",
        "operator_len",
        "move_type_prefix",
        "avg_qty",
        "qty_minus_avg",
    )
    .orderBy(F.col("operator_len").desc(), "qty_minus_avg")
)
