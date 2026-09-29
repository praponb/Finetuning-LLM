from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
)

screened = (
    base
    .crossJoin(base.agg(F.min("unit_price").alias("overall_min_unit_price")))
    .filter(F.col("unit_price") > F.col("overall_min_unit_price"))
    .drop("overall_min_unit_price")
)

with_parent_sku = (
    screened.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .filter(F.lower(F.col("sk.sku_code")).like("%l-%"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.unit_price",
        "s.operator_len",
        "sk.sku_code",
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

result = (
    with_parent_sku
    .cube("sku_id", "operator_len", "move_type")
    .agg(
        F.percentile_approx("unit_price", 0.5).alias("median_unit_price"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.count("*").alias("n_rows"),
    )
)
