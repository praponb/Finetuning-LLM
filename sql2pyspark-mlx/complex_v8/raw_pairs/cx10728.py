from pyspark.sql import functions as F

joined_ranges = (
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
        F.sum("b.unit_price").alias("prior_unit_price_7d"),
    )
)

with_children_moves = (
    joined_ranges.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price").between(32.1, 372.6)), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.moved_at",
        "s.prior_rows_7d",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.qty").alias("move_qty"),
    )
)

grouped = (
    with_children_moves
    .groupBy(
        F.year("move_moved_at").alias("move_moved_at_year"),
        "sku_id",
        F.date_trunc("month", "moved_at").alias("moved_at_month"),
    )
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.max("prior_rows_7d").alias("max_prior_rows_7d"),
        F.max("move_moved_at").alias("last_move_moved_at"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("move_unit_price"), 2).alias("stddev_move_unit_price"),
    )
)

with_details_sku = (
    grouped.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.category").isNotNull()), "left")
    .select(
        "s.move_moved_at_year",
        "s.sku_id",
        "s.moved_at_month",
        "s.first_moved_at",
        "s.max_prior_rows_7d",
        "s.row_count",
        "s.stddev_move_unit_price",
        F.col("sk.category").alias("sku_category"),
    )
)

result = (
    with_details_sku
    .groupBy("first_moved_at", "row_count")
    .pivot("sku_category", ["Toys", "Apparel", "Electronics"])
    .agg(F.sum("move_moved_at_year"))
)
