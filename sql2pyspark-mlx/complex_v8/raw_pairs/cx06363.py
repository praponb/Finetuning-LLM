from pyspark.sql import functions as F

base_skus = (
    df_skus
    .filter((F.col("weight_kg") < 21.0) & (F.col("is_active")))
    .select(
        "id",
        "sku_code",
        "weight_kg",
        "is_active",
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.sum("qty").alias("total_move_qty"),
        F.count(F.when(F.col("operator") == "op_chan", 1)).alias("move_operator_count"),
    )
)

summarized_rows = (
    base_skus.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.total_move_qty",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

with_activity = (
    summarized_rows
    .crossJoin(summarized_rows.agg(F.min("total_move_qty").alias("overall_min_total_move_qty")))
    .filter(F.col("total_move_qty") > F.col("overall_min_total_move_qty"))
    .drop("overall_min_total_move_qty")
)

result = (
    with_activity
    .rollup("is_active", "move_operator_count")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("n_rows"),
        F.round(F.stddev("move_operator_count"), 2).alias("stddev_move_operator_count"),
    )
)
