from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("sku_id").orderBy("qty", "id")

cleaned_moves = (
    df_stock_moves
    .filter((F.col("move_type").isin("OUTBOUND", "INBOUND")) & (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
    )
)

joined_sku = (
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .filter(F.array_contains(F.col("sk.labels"), "express"))
    .select(
        "s.*",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("w.city").alias("warehouse_city"),
    )
)

latest_per_key = (
    joined_sku
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .select(
        "id",
        "sku_id",
        "move_type",
        "sku_unit_cost",
        "sku_labels",
        F.round(F.col("qty") / F.nullif(F.col("sku_unit_cost"), F.lit(0)), 3).alias("qty_per_sku_unit_cost"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
    .orderBy(
        F.col("qty_per_sku_unit_cost").desc_nulls_first(),
        "operator_or_unknown",
        "id",
    )
    .limit(5)
)
