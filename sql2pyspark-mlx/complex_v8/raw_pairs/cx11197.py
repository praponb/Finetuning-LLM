from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_wh_code_len = Window.partitionBy("city", "wh_code_len")
w_wh_code_len_opened_on_month = Window.partitionBy("wh_code_len", F.date_trunc("month", "opened_on")).orderBy(F.col("capacity_bucket").desc(), "id")
w_city_by_capacity_bucket_prio = Window.partitionBy("city").orderBy("capacity_bucket", "id").rowsBetween(-1, -1)
w_city_by_capacity_bucket_full = Window.partitionBy("city").orderBy("capacity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_warehouses
    .filter((F.col("city") == "Singapore") & (F.col("opened_on").isNotNull()))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        (F.floor(F.col("capacity") / 5000) * 5000).alias("capacity_bucket"),
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
        F.length(F.trim(F.col("wh_code"))).alias("wh_code_len"),
    )
)

scored = (
    scoped
    .select(
        "*",
        F.upper(F.trim(F.col("city"))).alias("city_clean"),
        F.substring(F.trim(F.col("wh_code")), 1, 2).alias("wh_code_prefix"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.sum("capacity").over(w_city_wh_code_len).alias("city_wh_code_len_sum_capacity"),
        (F.col("capacity_filled") - F.lag("capacity_filled").over(w_wh_code_len_opened_on_month)).alias("capacity_filled_change"),
        F.max("capacity_filled").over(w_city_by_capacity_bucket_prio).alias("capacity_filled_max_1_prior"),
        F.last("opened_on").over(w_city_by_capacity_bucket_full).alias("last_opened_on"),
        F.round(F.col("wh_code_len") / F.nullif(F.sum("wh_code_len").over(w_city_wh_code_len), F.lit(0)), 4).alias("wh_code_len_share_of_city_wh_code_len"),
        F.sum("capacity_bucket").over(w_city_wh_code_len).alias("city_wh_code_len_sum_capacity_bucket"),
    )
)

tagged = (
    with_windows
    .select(
        "*",
        F.coalesce(F.col("city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.round(F.col("capacity") / F.nullif(F.col("capacity_filled"), F.lit(0)), 3).alias("capacity_per_capacity_filled"),
    )
)

city_or_unknown_stats = (
    tagged
    .groupBy("city_or_unknown")
    .agg(F.avg("capacity").alias("avg_capacity"))
)

compared = (
    tagged.alias("s")
    .join(city_or_unknown_stats.alias("g"), F.col("g.city_or_unknown") == F.col("s.city_or_unknown"), "inner")
    .filter(F.col("s.capacity") > F.col("g.avg_capacity"))
    .select(
        "s.*",
        "g.avg_capacity",
        F.round(F.col("s.capacity") - F.col("g.avg_capacity"), 2).alias("capacity_minus_avg"),
    )
)

without_matches = (
    compared.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        "capacity_bucket",
        "wh_code_len",
        "city_clean",
        "wh_code_prefix",
        "capacity_filled_change",
        "capacity_filled_max_1_prior",
        "last_opened_on",
        "wh_code_len_share_of_city_wh_code_len",
        "city_wh_code_len_sum_capacity_bucket",
        "city_or_unknown",
        "capacity_per_capacity_filled",
        "avg_capacity",
        "capacity_minus_avg",
        F.round(F.col("capacity") / F.nullif(F.col("capacity_filled"), F.lit(0)), 3).alias("capacity_per_capacity_filled_2"),
        F.abs(F.col("capacity_filled_change") - 0).alias("capacity_filled_change_distance"),
    )
    .orderBy(F.col("city").asc())
)
