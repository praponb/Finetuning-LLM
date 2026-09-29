from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city = Window.partitionBy("city")
w_all_by_capacity_ties = Window.orderBy(F.col("capacity").desc())

filtered = df_warehouses.filter(F.col("opened_on").isNotNull())

annotated = (
    filtered
    .select(
        "*",
        F.date_trunc("month", F.col("opened_on")).alias("opened_on_month"),
        F.lower(F.regexp_replace(F.trim(F.col("city")), " ", "_")).alias("city_slug"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.min("unit_price").alias("min_move_unit_price"))
)

summarized_rows = (
    annotated.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select("s.*", "st.min_move_unit_price")
)

metrics = (
    summarized_rows
    .select(
        "*",
        F.round(F.col("min_move_unit_price") / F.nullif(F.sum("min_move_unit_price").over(w_city), F.lit(0)), 4).alias("min_move_unit_price_share_of_city"),
        F.dense_rank().over(w_all_by_capacity_ties).alias("capacity_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_capacity_ties), 3).alias("capacity_cume_dist"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.sum("qty").alias("total_move_qty"),
        F.max("qty").alias("max_move_qty"),
        F.min("moved_at").alias("first_moved_at"),
    )
)

with_counts = (
    metrics.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "st.total_move_qty",
        "st.max_move_qty",
        "st.first_moved_at",
    )
)

summary = (
    with_counts
    .groupBy(
        "city",
        F.date_format("opened_on_month", "yyyy-MM").alias("opened_on_month_ym"),
    )
    .agg(
        F.count(F.when(F.col("city") != "Singapore", 1)).alias("n_city_singapore"),
        F.countDistinct("city").alias("distinct_citys"),
        F.sum(F.when(F.col("city").isin("Singapore", "Kuala Lumpur"), 1).otherwise(0)).alias("n_city_in_singapore_kuala_lump"),
        F.max("opened_on_month").alias("last_opened_on_month"),
        F.sum("min_move_unit_price_share_of_city").alias("sum_min_move_unit_price_share_of_city_check"),
    )
    .filter(F.col("sum_min_move_unit_price_share_of_city_check") > 0.2)
    .drop("sum_min_move_unit_price_share_of_city_check")
)

result = (
    summary
    .select(
        "city",
        "opened_on_month_ym",
        "distinct_citys",
        "n_city_in_singapore_kuala_lump",
    )
    .orderBy(F.col("city").desc())
)
