from pyspark.sql import functions as F

prepared = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

result = (
    prepared
    .groupingSets(
        [["operator", "sku_id"], ["operator"], ["sku_id"], []],
        "operator", "sku_id",
    )
    .agg(F.countDistinct("id").alias("distinct_ids"))
    .select(
        F.coalesce(F.col("operator"), F.lit("ALL")).alias("operator"),
        "sku_id",
        "distinct_ids",
    )
)
