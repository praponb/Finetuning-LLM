from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("city_group", "city").orderBy(F.col("first_moved_at").desc(), F.col("id").desc())

base_warehouses = (
    df_warehouses
    .filter(F.col("capacity") > 2614)
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.length(F.trim(F.col("wh_code"))).alias("wh_code_len"),
        F.when(F.col("city").isin("Kuala Lumpur", "Bangkok"), "group_a").when(F.col("city") == "Singapore", "group_b").otherwise("other").alias("city_group"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.min("qty").alias("min_move_qty"),
    )
)

with_counts = (
    base_warehouses.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select("s.*", "st.first_moved_at", "st.min_move_qty")
)

latest_per_key = (
    with_counts
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .cube("city", "city_group", "wh_code_len")
    .agg(
        F.round(F.sum("min_move_qty") / F.nullif(F.sum("capacity"), F.lit(0)), 3).alias("min_move_qty_to_capacity_ratio"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("min_move_qty").alias("min_min_move_qty"),
    )
    .select(
        F.coalesce(F.col("city"), F.lit("ALL")).alias("city"),
        F.coalesce(F.col("city_group"), F.lit("ALL")).alias("city_group"),
        "wh_code_len",
        "min_move_qty_to_capacity_ratio",
        "distinct_ids",
        "min_min_move_qty",
    )
    .orderBy(
        F.col("city").asc_nulls_last(),
        F.col("city_group").asc_nulls_last(),
        F.col("wh_code_len").asc_nulls_last(),
    )
)
