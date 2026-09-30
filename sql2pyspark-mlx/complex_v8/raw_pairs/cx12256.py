from pyspark.sql import functions as F

in_window = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.qty").alias("prior_qty_7d"),
    )
)

with_details_sku = (
    in_window.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.labels").alias("sku_labels"),
        "sk.sku_code",
    )
)

screened = (
    with_details_sku.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.warehouse_id") >= 3), "left_anti")
)

result = (
    screened
    .select(
        "id",
        "sku_id",
        "moved_at",
        "sku_weight_kg",
        "sku_labels",
        F.length(F.trim(F.col("sku_code"))).alias("sku_code_len"),
        F.upper(F.trim(F.col("sku_code"))).alias("sku_code_clean"),
    )
)
