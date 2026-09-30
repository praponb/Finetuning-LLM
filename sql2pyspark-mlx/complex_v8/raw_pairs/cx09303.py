from pyspark.sql import functions as F

in_window = (
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

grouped = (
    in_window
    .groupBy("sku_id")
    .agg(
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.min("moved_at").alias("first_moved_at"),
    )
)

with_parent_sku = (
    grouped.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.unit_cost") >= 108.5), "inner")
    .select(
        "s.sku_id",
        "s.distinct_sku_ids",
        F.col("sk.unit_cost").alias("sku_unit_cost"),
        "sk.sku_code",
    )
)

result = (
    with_parent_sku
    .filter(F.col("sku_unit_cost") > 60.9)
    .select(
        "sku_id",
        "distinct_sku_ids",
        "sku_unit_cost",
        "sku_code",
    )
    .orderBy(
        F.col("distinct_sku_ids").asc(),
        F.col("sku_unit_cost").desc_nulls_first(),
    )
)
