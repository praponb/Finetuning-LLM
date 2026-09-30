from pyspark.sql import functions as F

covered = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.unit_price").alias("prior_unit_price_3d"),
    )
)

exploded_detail_moves = (
    covered.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.prior_rows_3d",
        "s.prior_unit_price_3d",
        F.col("st.id").alias("move_id"),
        "st.move_type",
        F.col("st.operator").alias("move_operator"),
    )
)

result = (
    exploded_detail_moves
    .select(
        "id",
        "sku_id",
        "prior_rows_3d",
        "prior_unit_price_3d",
        "move_id",
        "move_type",
        "move_operator",
        F.initcap(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
    .orderBy(F.col("prior_rows_3d").desc_nulls_first(), "id", "move_id")
    .limit(5)
)
