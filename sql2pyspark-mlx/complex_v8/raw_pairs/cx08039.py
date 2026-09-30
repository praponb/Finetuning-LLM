from pyspark.sql import functions as F

in_window = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.unit_price").alias("prior_unit_price_12h"),
    )
)

with_details = (
    in_window.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

with_parent = (
    with_details.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.*",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.labels").alias("sku_labels_2"),
        "sk.sku_code",
    )
)

without_matches = (
    with_parent.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.warehouse_id").isNotNull()) & (F.col("st.unit_price") <= 290.4), "left_anti")
)

result = (
    without_matches
    .select("id", "prior_unit_price_12h", "sku_weight_kg")
    .unpivot(
        ids=["id"],
        values=["prior_unit_price_12h", "sku_weight_kg"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .orderBy("id", "metric_name")
)
