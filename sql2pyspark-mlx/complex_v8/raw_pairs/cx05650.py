from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_operator_by_opened_on_t = Window.partitionBy("move_operator").orderBy("opened_on")
w_opened_on_month_move_operato = Window.partitionBy(F.date_trunc("month", "opened_on"), "move_operator")
w_move_operator_by_opened_on = Window.partitionBy("move_operator").orderBy("opened_on", "id", "move_id")

prepared_warehouses = (
    df_warehouses
    .filter(F.lower(F.col("wh_code")).like("%x1%"))
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        F.length(F.trim(F.col("city"))).alias("city_len"),
    )
)

city_stats = (
    prepared_warehouses
    .groupBy("city")
    .agg(F.avg("city_len").alias("avg_city_len"))
)

outliers = (
    prepared_warehouses.alias("s")
    .join(city_stats.alias("g"), F.col("g.city") == F.col("s.city"), "inner")
    .filter(F.col("s.city_len") > F.col("g.avg_city_len"))
    .select(
        "s.*",
        "g.avg_city_len",
        F.round(F.col("s.city_len") - F.col("g.avg_city_len"), 2).alias("city_len_minus_avg"),
    )
)

detail_rows_moves = (
    outliers.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.move_type").isNotNull()), "inner")
    .select(
        "s.id",
        "s.city",
        "s.opened_on",
        "s.city_len",
        "s.avg_city_len",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.operator").alias("move_operator"),
        "st.move_type",
        F.col("st.sku_id").alias("move_sku_id"),
    )
)

with_details = (
    detail_rows_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.move_sku_id"), "inner")
    .select(
        "s.*",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

windowed = (
    with_details
    .select(
        "*",
        F.dense_rank().over(w_move_operator_by_opened_on_t).alias("opened_on_dense_rank"),
        F.count(F.lit(1)).over(w_opened_on_month_move_operato).alias("rows_in_opened_on_month_move_operator"),
        F.size(F.collect_set("city").over(w_opened_on_month_move_operato)).alias("distinct_city_in_opened_on_month_move_operator"),
        F.row_number().over(w_move_operator_by_opened_on).alias("seq_no"),
        F.max("avg_city_len").over(w_opened_on_month_move_operato).alias("opened_on_month_move_operator_max_avg_city_len"),
        F.min("move_unit_price").over(w_opened_on_month_move_operato).alias("opened_on_month_move_operator_min_move_unit_pric"),
    )
)

result = (
    windowed
    .select(
        "id",
        "city",
        "opened_on",
        "city_len",
        "avg_city_len",
        "move_id",
        "move_unit_price",
        "move_operator",
        "sku_category",
        "sku_is_active",
        "opened_on_dense_rank",
        "distinct_city_in_opened_on_month_move_operator",
        "seq_no",
        "opened_on_month_move_operator_max_avg_city_len",
        "opened_on_month_move_operator_min_move_unit_pric",
    )
    .orderBy(F.col("move_id").desc(), F.col("city").asc_nulls_last())
)
