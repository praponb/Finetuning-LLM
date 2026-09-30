from pyspark.sql import functions as F

in_window = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.qty").alias("prior_qty_1d"),
    )
)

with_details = (
    in_window.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(~F.col("sk.is_active"))
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        "s.prior_rows_1d",
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

detail_rows = (
    with_details.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.sku_id",
        "s.prior_rows_1d",
        "s.sku_is_active",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.operator").alias("move_operator"),
    )
)

tagged = (
    detail_rows
    .filter(F.col("move_unit_price").between(112.8, 367.6))
    .withColumn("move_operator_prefix", F.substring(F.trim(F.col("move_operator")), 1, 4))
)

result = (
    tagged
    .groupBy("prior_rows_1d")
    .pivot("move_operator", ["op_eve", "op_dew", "op_ann"])
    .agg(F.max("move_unit_price"))
    .orderBy("prior_rows_1d")
)
