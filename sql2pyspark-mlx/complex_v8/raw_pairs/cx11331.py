from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter((F.col("qty") <= 349) & (F.to_date(F.col("moved_at")) < F.lit("2025-03-08").cast("date")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
    )
)

combined_details = (
    base.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.qty_plus_unit_price",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

labeled = (
    combined_details
    .withColumn("qty_plus_qty_plus_unit_price", F.col("qty") + F.col("qty_plus_unit_price"))
)

scored = (
    labeled
    .filter(~F.col("move_type").isin("INBOUND", "ADJUST"))
    .select(
        "id",
        "move_type",
        "qty",
        "qty_plus_unit_price",
        "sku_category",
        "sku_is_active",
        "qty_plus_qty_plus_unit_price",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
)

result = (
    scored
    .select(
        "id",
        "move_type",
        "qty",
        "qty_plus_unit_price",
        "sku_is_active",
        "qty_plus_qty_plus_unit_price",
        "qty_bucket",
        F.when(F.col("sku_category").isin("Electronics", "Food"), "group_a").when(F.col("sku_category") == "Toys", "group_b").otherwise("other").alias("sku_category_group"),
    )
    .orderBy("qty_plus_unit_price", "id")
    .limit(5)
)
