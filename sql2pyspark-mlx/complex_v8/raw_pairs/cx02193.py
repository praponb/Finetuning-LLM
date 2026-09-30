from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("city").orderBy(F.col("capacity_bucket").desc(), F.col("id").desc())
w_opened_on_year_opened_on_mon = Window.partitionBy(F.year("opened_on"), "opened_on_month").orderBy("opened_on", "id")
w_opened_on_month_by_move_move = Window.partitionBy("opened_on_month").orderBy("move_move_type_count", "id")
w_move_move_type_count_opened = Window.partitionBy("move_move_type_count", F.year("opened_on"))
w_opened_on_month_by_move_move_2 = Window.partitionBy("opened_on_month").orderBy("move_move_type_count", "id").rowsBetween(-1, Window.currentRow)

scoped = (
    df_warehouses
    .filter(F.col("opened_on") >= F.lit("2024-11-25").cast("date"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        (F.floor(F.col("capacity") / 5000) * 5000).alias("capacity_bucket"),
        F.upper(F.trim(F.col("wh_code"))).alias("wh_code_clean"),
        F.month(F.col("opened_on")).alias("opened_on_month"),
    )
)

deduped = (
    scoped
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(~F.col("move_type").isin("INBOUND", "OUTBOUND"), 1)).alias("move_move_type_count"),
    )
)

with_stats = (
    deduped.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

with_trends = (
    with_stats
    .select(
        "*",
        F.row_number().over(w_opened_on_year_opened_on_mon).alias("rn"),
        F.datediff(F.col("opened_on"), F.lag("opened_on").over(w_opened_on_month_by_move_move)).alias("days_since_prev_opened_on"),
        F.count(F.lit(1)).over(w_move_move_type_count_opened).alias("rows_in_move_move_type_count_opened_on_year"),
        F.round(F.avg("move_move_type_count").over(w_opened_on_month_by_move_move_2), 2).alias("move_move_type_count_avg_2"),
        F.round(F.col("capacity_bucket") - F.avg("capacity_bucket").over(w_move_move_type_count_opened), 2).alias("capacity_bucket_vs_move_move_type_count_opened_o"),
    )
)

city_stats = (
    with_trends
    .groupBy("city")
    .agg(F.percentile_approx("opened_on_month", 0.5).alias("median_opened_on_month"))
)

outliers = (
    with_trends.alias("s")
    .join(city_stats.alias("g"), F.col("g.city") == F.col("s.city"), "inner")
    .filter(F.col("s.opened_on_month") >= F.col("g.median_opened_on_month"))
    .select(
        "s.*",
        "g.median_opened_on_month",
        F.round(F.col("s.opened_on_month") - F.col("g.median_opened_on_month"), 2).alias("opened_on_month_minus_median"),
    )
)

screened = (
    outliers.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left_semi")
)

result = (
    screened
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        "capacity_bucket",
        "wh_code_clean",
        "opened_on_month",
        "rn",
        "days_since_prev_opened_on",
        "rows_in_move_move_type_count_opened_on_year",
        "capacity_bucket_vs_move_move_type_count_opened_o",
        "opened_on_month_minus_median",
    )
    .orderBy(F.col("capacity_bucket").desc(), F.col("wh_code_clean").asc())
)
