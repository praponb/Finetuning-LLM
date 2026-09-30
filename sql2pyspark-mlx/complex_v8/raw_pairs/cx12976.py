from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_year_prior_rows_12h = Window.partitionBy(F.year("moved_at"), "prior_rows_12h")
w_all_by_prior_qty_12h_full = Window.orderBy(F.col("prior_qty_12h").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_prior_qty_12h = Window.orderBy(F.col("prior_qty_12h").desc(), "id")
w_all_by_prior_qty_12h_ties = Window.orderBy(F.col("prior_qty_12h").desc())

covered = (
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
        F.sum("b.qty").alias("prior_qty_12h"),
    )
)

tagged = (
    covered
    .filter(F.col("sku_id") >= 4)
    .select(
        "*",
        F.coalesce(F.col("prior_rows_12h"), F.lit(0)).alias("prior_rows_12h_filled"),
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
    )
)

with_activity = (
    tagged.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.sku_code").isNotNull()), "left_semi")
)

windowed = (
    with_activity
    .select(
        "*",
        F.round(F.col("prior_rows_12h_filled") - F.avg("prior_rows_12h_filled").over(w_moved_at_year_prior_rows_12h), 2).alias("prior_rows_12h_filled_vs_moved_at_year_prior_row"),
        F.nth_value("prior_rows_12h", 2).over(w_all_by_prior_qty_12h_full).alias("second_prior_rows_12h"),
        F.max("prior_rows_12h_filled").over(w_moved_at_year_prior_rows_12h).alias("moved_at_year_prior_rows_12h_max_prior_rows_12h_"),
        F.row_number().over(w_all_by_prior_qty_12h).alias("seq_no"),
        F.round(F.cume_dist().over(w_all_by_prior_qty_12h_ties), 3).alias("prior_qty_12h_cume_dist"),
        F.size(F.collect_set("sku_id").over(w_moved_at_year_prior_rows_12h)).alias("distinct_sku_id_in_moved_at_year_prior_rows_12h"),
    )
)

result = (
    windowed
    .select(
        "id",
        "sku_id",
        "prior_rows_12h",
        "prior_qty_12h",
        "moved_at_label",
        "prior_rows_12h_filled_vs_moved_at_year_prior_row",
        "second_prior_rows_12h",
        "moved_at_year_prior_rows_12h_max_prior_rows_12h_",
        "seq_no",
        "distinct_sku_id_in_moved_at_year_prior_rows_12h",
        F.when(F.col("prior_rows_12h_filled") >= 6, "large").when(F.col("prior_rows_12h_filled") >= 3, "medium").otherwise("small").alias("prior_rows_12h_filled_tier"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
    .orderBy(F.col("prior_rows_12h").asc(), "seq_no")
)
