from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city = Window.partitionBy("city")
w_all_by_capacity = Window.orderBy("capacity", "id")
w_dedup = Window.partitionBy("city").orderBy("days_since_prev_opened_on", "id")

prepared_warehouses = (
    df_warehouses
    .filter(F.col("city") == "Singapore")
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.lower(F.regexp_replace(F.trim(F.col("city")), " ", "_")).alias("city_slug"),
        F.abs(F.col("capacity") - 11005).alias("capacity_distance"),
        F.length(F.trim(F.col("wh_code"))).alias("wh_code_len"),
    )
)

ranked = (
    prepared_warehouses
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
        "city_slug",
        "wh_code_len",
        F.count(F.lit(1)).over(w_city).alias("rows_in_city"),
        F.datediff(F.col("opened_on"), F.lag("opened_on").over(w_all_by_capacity)).alias("days_since_prev_opened_on"),
    )
)

qualified = (
    ranked
    .crossJoin(ranked.agg(F.max("days_since_prev_opened_on").alias("overall_max_days_since_prev_opened_on")))
    .filter(F.col("days_since_prev_opened_on") < F.col("overall_max_days_since_prev_opened_on"))
    .drop("overall_max_days_since_prev_opened_on")
)

unique_rows = (
    qualified
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

labeled = (
    unique_rows
    .select(
        "*",
        F.date_trunc("quarter", F.col("opened_on")).alias("opened_on_quarter"),
        F.substring(F.trim(F.col("city")), 1, 2).alias("city_prefix"),
    )
)

exploded_detail_moves = (
    labeled.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.wh_code",
        "s.city",
        "s.opened_on",
        "s.city_slug",
        "s.wh_code_len",
        "s.days_since_prev_opened_on",
        "s.opened_on_quarter",
        "s.city_prefix",
        F.col("st.id").alias("move_id"),
        "st.move_type",
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.qty").alias("move_qty"),
    )
)

without_matches = (
    exploded_detail_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left_semi")
    .filter(F.upper(F.trim(F.col("wh_code"))).like("B%"))
)

result = (
    without_matches
    .select(
        "id",
        "city",
        "city_slug",
        "days_since_prev_opened_on",
        "opened_on_quarter",
        "city_prefix",
        "move_type",
        "move_sku_id",
        F.month(F.col("opened_on")).alias("opened_on_month"),
    )
    .orderBy("city")
)
