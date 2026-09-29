from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id_prior_rows_1d = Window.partitionBy("sku_id", "prior_rows_1d")
w_sku_id_by_prior_rows_1d_ties = Window.partitionBy("sku_id").orderBy(F.col("prior_rows_1d").desc())
w_sku_id_by_prior_rows_1d = Window.partitionBy("sku_id").orderBy(F.col("prior_rows_1d").desc(), "id")

joined_ranges = (
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
    joined_ranges
    .select(
        "id",
        "sku_id",
        "prior_rows_1d",
        F.round(F.col("prior_unit_price_1d") - F.avg("prior_unit_price_1d").over(w_sku_id_prior_rows_1d), 2).alias("prior_unit_price_1d_vs_sku_id_prior_rows_1d_avg"),
        F.round(F.cume_dist().over(w_sku_id_by_prior_rows_1d_ties), 3).alias("prior_rows_1d_cume_dist"),
        F.round(F.col("prior_rows_1d") / F.nullif(F.sum("prior_rows_1d").over(w_sku_id_prior_rows_1d), F.lit(0)), 4).alias("prior_rows_1d_share_of_sku_id_prior_rows_1d"),
        F.round(F.percent_rank().over(w_sku_id_by_prior_rows_1d), 3).alias("prior_rows_1d_percent_rank"),
        F.dense_rank().over(w_sku_id_by_prior_rows_1d_ties).alias("prior_rows_1d_dense_rank"),
    )
)

with_parent = (
    windowed.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select("s.*", F.col("sk.is_active").alias("sku_is_active"), "sk.sku_code")
)

totals = (
    with_parent
    .groupBy("sku_id", "prior_rows_1d")
    .agg(
        F.round(F.sum("prior_rows_1d_cume_dist"), 2).alias("total_prior_rows_1d_cume_dist"),
        F.round(F.avg("prior_unit_price_1d_vs_sku_id_prior_rows_1d_avg"), 2).alias("avg_prior_unit_price_1d_vs_sku_id_prior_rows_1d_"),
        F.count(F.when(F.upper(F.trim(F.col("sku_code"))).like("E%"), 1)).alias("n_upper_trim_sku_code_like_e"),
        F.round(F.sum("prior_unit_price_1d_vs_sku_id_prior_rows_1d_avg") / F.nullif(F.sum("prior_rows_1d_cume_dist"), F.lit(0)), 3).alias("prior_unit_price_1d_vs_sku_id_prior_rows_1d_avg_"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("sku_is_active"), 1)).alias("n_sku_is_active_true"),
        F.max("prior_rows_1d_percent_rank").alias("max_prior_rows_1d_percent_rank_check"),
    )
    .filter(F.col("max_prior_rows_1d_percent_rank_check") > 0.1)
    .drop("max_prior_rows_1d_percent_rank_check")
)

result = (
    totals
    .filter(F.col("n_upper_trim_sku_code_like_e") <= 6)
    .select(
        "sku_id",
        "prior_rows_1d",
        "n_upper_trim_sku_code_like_e",
        "prior_unit_price_1d_vs_sku_id_prior_rows_1d_avg_",
        "row_count",
        F.when(F.col("n_sku_is_active_true") >= 8, "HIGH").when(F.col("n_sku_is_active_true") >= 2, "MEDIUM").otherwise("LOW").alias("n_sku_is_active_true_tier"),
    )
    .orderBy(F.col("sku_id").asc())
)
