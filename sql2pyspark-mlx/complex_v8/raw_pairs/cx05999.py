from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("operator", "move_type").orderBy(F.col("unit_price_bucket").desc(), F.col("id").desc())
w_moved_at_year_sku_id = Window.partitionBy(F.year("moved_at"), "sku_id")

prepared = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
        F.concat(F.col("operator"), F.lit("#"), F.col("qty").cast("string")).alias("operator_ref"),
    )
)

labeled = (
    prepared
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "qty_filled",
        "moved_at_label",
        "operator_ref",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

latest_per_key = (
    labeled
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

ranked = (
    latest_per_key
    .select(
        "*",
        F.round(F.col("qty_filled") / F.nullif(F.sum("qty_filled").over(w_moved_at_year_sku_id), F.lit(0)), 4).alias("qty_filled_share_of_moved_at_year_sku_id"),
        F.sum("qty").over(w_moved_at_year_sku_id).alias("moved_at_year_sku_id_sum_qty"),
    )
)

enriched = (
    ranked
    .filter(F.col("qty") > 78)
    .select(
        "*",
        F.least(F.col("qty_filled"), F.col("qty")).alias("least_qty_filled_qty"),
        F.concat(F.col("moved_at_label"), F.lit("#"), F.col("id").cast("string")).alias("moved_at_label_ref"),
    )
)

result = (
    enriched
    .groupBy("operator")
    .pivot("move_type", ["ADJUST", "TRANSFER", "OUTBOUND"])
    .agg(F.count("moved_at_year_sku_id_sum_qty"))
)
