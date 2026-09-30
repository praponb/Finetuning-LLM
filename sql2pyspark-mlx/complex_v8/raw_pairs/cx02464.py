from pyspark.sql import functions as F

filtered = df_stock_moves.filter(F.col("qty") <= 256)

without_matches = (
    filtered
    .crossJoin(filtered.agg(F.avg("unit_price").alias("overall_avg_unit_price")))
    .filter(F.col("unit_price") > F.col("overall_avg_unit_price"))
    .drop("overall_avg_unit_price")
)

without_matches_2 = (
    without_matches.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.warehouse_id") >= 3) & (F.col("st.qty").between(78, 394)), "left_anti")
    .filter(F.col("qty") >= 169)
)

with_parent = (
    without_matches_2.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        F.col("sk.is_active").alias("sku_is_active"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

with_parent_sku = (
    with_parent.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        "s.sku_unit_cost",
        "s.sku_is_active",
        F.col("sk.labels").alias("sku_labels_2"),
    )
)

stats = (
    with_parent_sku
    .groupBy("warehouse_id", "sku_id", "sku_is_active")
    .agg(
        F.round(F.stddev("sku_unit_cost"), 2).alias("stddev_sku_unit_cost"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.max("unit_price").alias("max_unit_price"),
        F.count("*").alias("row_count"),
    )
)

joined = (
    stats.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.warehouse_id",
        "s.sku_id",
        "s.sku_is_active",
        "s.stddev_sku_unit_cost",
        "s.max_unit_price",
        "s.row_count",
        F.col("sk.is_active").alias("sku_is_active_2"),
        "sk.sku_code",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

result = (
    joined
    .select(
        "warehouse_id",
        "sku_id",
        "max_unit_price",
        "stddev_sku_unit_cost",
    )
    .unpivot(
        ids=["warehouse_id", "sku_id"],
        values=["max_unit_price", "stddev_sku_unit_cost"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("warehouse_id", "sku_id", "measure")
)
