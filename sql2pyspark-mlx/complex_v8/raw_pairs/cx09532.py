from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_prior_rows_1d = Window.partitionBy("sku_id", "prior_rows_1d")
w_moved_at_to_date_by_moved_at = Window.partitionBy(F.to_date("moved_at")).orderBy("moved_at", "id").rowsBetween(-6, Window.currentRow)

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
        F.sum("b.unit_price").alias("prior_unit_price_1d"),
    )
)

windowed = (
    in_window
    .select(
        "*",
        F.count(F.lit(1)).over(w_sku_id_prior_rows_1d).alias("rows_in_sku_id_prior_rows_1d"),
        F.round(F.col("prior_rows_1d") - F.avg("prior_rows_1d").over(w_sku_id_prior_rows_1d), 2).alias("prior_rows_1d_vs_sku_id_prior_rows_1d_avg"),
        F.round(F.avg("prior_rows_1d").over(w_moved_at_to_date_by_moved_at), 2).alias("prior_rows_1d_avg_7"),
    )
)

stats = (
    windowed
    .groupBy(
        F.when(F.col("prior_unit_price_1d") >= 2000.0, "large").when(F.col("prior_unit_price_1d") >= 1000.0, "medium").otherwise("small").alias("prior_unit_price_1d_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("prior_rows_1d_avg_7") / F.nullif(F.sum("rows_in_sku_id_prior_rows_1d"), F.lit(0)), 3).alias("prior_rows_1d_avg_7_to_rows_in_sku_id_prior_rows"),
    )
)

result = (
    stats
    .select(
        "prior_unit_price_1d_tier",
        "cnt",
        "prior_rows_1d_avg_7_to_rows_in_sku_id_prior_rows",
        (F.floor(F.col("cnt") / 2) * 2).alias("cnt_bucket"),
    )
    .orderBy(
        F.col("prior_rows_1d_avg_7_to_rows_in_sku_id_prior_rows").desc(),
        F.col("cnt").desc(),
    )
)
