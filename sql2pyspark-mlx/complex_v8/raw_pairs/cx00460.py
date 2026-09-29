from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("operator", "sku_id").orderBy(F.col("least_least_qty_unit_price_qty").desc(), F.col("id").desc())

recent = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 2)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.quarter(F.col("moved_at")).alias("moved_at_quarter"),
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
    )
)

enriched = (
    recent
    .select(
        "*",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.least(F.col("least_qty_unit_price"), F.col("qty")).alias("least_least_qty_unit_price_qty"),
        F.concat_ws("|", F.col("operator"), F.col("operator_prefix")).alias("operator_operator_prefix_label"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"), F.count("*").alias("move_count_2"))
)

with_stats = (
    enriched.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

latest_per_key = (
    with_stats
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .groupingSets(
        [["moved_at_quarter", "move_count_2", "move_count"], ["moved_at_quarter"]],
        "moved_at_quarter", "move_count_2", "move_count",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.round(F.sum("least_qty_unit_price") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("least_qty_unit_price_to_qty_ratio"),
        F.percentile_approx("least_least_qty_unit_price_qty", 0.5).alias("median_least_least_qty_unit_price_qty"),
    )
    .orderBy(
        F.col("moved_at_quarter").asc_nulls_last(),
        F.col("move_count_2").asc_nulls_last(),
        F.col("move_count").asc_nulls_last(),
    )
)
