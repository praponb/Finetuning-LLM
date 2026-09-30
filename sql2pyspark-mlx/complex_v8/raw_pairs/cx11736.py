from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_count_by_least_unit_pri = Window.partitionBy("move_count").orderBy(F.col("least_unit_price_qty_bucket").desc_nulls_first(), "id")
w_sku_is_active_by_least_unit = Window.partitionBy("sku_is_active").orderBy(F.col("least_unit_price_qty").desc())

filtered = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("qty").cast("string")).alias("move_type_ref"),
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
    )
)

annotated = (
    filtered
    .select(
        "id",
        "sku_id",
        "move_type",
        "operator",
        "move_type_ref",
        "least_unit_price_qty",
        F.greatest(F.col("least_unit_price_qty"), F.col("unit_price")).alias("greatest_least_unit_price_qty_unit_price"),
    )
)

enriched = (
    annotated
    .filter(F.col("move_type") == "TRANSFER")
    .select(
        "id",
        "sku_id",
        "move_type",
        "move_type_ref",
        "least_unit_price_qty",
        "greatest_least_unit_price_qty_unit_price",
        (F.floor(F.col("least_unit_price_qty") / 100) * 100).alias("least_unit_price_qty_bucket"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.max("moved_at").alias("last_moved_at_2"),
        F.count("*").alias("move_count"),
    )
)

summarized_rows = (
    enriched.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        "st.last_moved_at_2",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

combined_details_sku = (
    summarized_rows.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select("s.*", F.col("sk.is_active").alias("sku_is_active"))
)

metrics = (
    combined_details_sku
    .select(
        "id",
        "sku_id",
        "move_type_ref",
        "greatest_least_unit_price_qty_unit_price",
        "least_unit_price_qty_bucket",
        "last_moved_at",
        "move_count",
        F.lag("last_moved_at_2").over(w_move_count_by_least_unit_pri).alias("prev_last_moved_at_2"),
        F.round(F.percent_rank().over(w_sku_is_active_by_least_unit), 3).alias("least_unit_price_qty_percent_rank"),
        F.dense_rank().over(w_sku_is_active_by_least_unit).alias("least_unit_price_qty_dense_rank"),
    )
)

summary = (
    metrics
    .filter(F.col("least_unit_price_qty_dense_rank") > 2)
    .groupBy(
        F.date_trunc("month", "last_moved_at").alias("last_moved_at_month"),
        "least_unit_price_qty_dense_rank",
    )
    .agg(
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.round(F.avg("least_unit_price_qty_percent_rank"), 2).alias("avg_least_unit_price_qty_percent_rank"),
        F.max("prev_last_moved_at_2").alias("last_prev_last_moved_at_2"),
        F.min("greatest_least_unit_price_qty_unit_price").alias("min_greatest_least_unit_price_qty_unit_price"),
        F.max("greatest_least_unit_price_qty_unit_price").alias("max_greatest_least_unit_price_qty_unit_price_che"),
    )
    .filter(F.col("max_greatest_least_unit_price_qty_unit_price_che") > 100.8)
    .drop("max_greatest_least_unit_price_qty_unit_price_che")
)

result = (
    summary
    .select(
        "last_moved_at_month",
        "distinct_sku_ids",
        "avg_least_unit_price_qty_percent_rank",
        "last_prev_last_moved_at_2",
        F.date_format(F.col("last_moved_at_month"), "yyyy-MM-dd HH:00").alias("last_moved_at_month_label"),
    )
    .orderBy(
        F.col("avg_least_unit_price_qty_percent_rank").desc(),
        "last_moved_at_month",
    )
)
