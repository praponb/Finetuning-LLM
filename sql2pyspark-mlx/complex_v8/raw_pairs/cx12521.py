from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_opened_on_quarter_run = Window.orderBy("opened_on_quarter", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_id_move_operator = Window.partitionBy("move_id", "move_operator")
w_all_by_opened_on_quarter_pri = Window.orderBy("opened_on_quarter", "id", "move_id").rowsBetween(-3, -1)
w_all_by_opened_on_quarter_ful = Window.orderBy("opened_on_quarter", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_opened_on_quarter = Window.orderBy("opened_on_quarter", "id", "move_id")
w_move_id_move_sku_id_by_move = Window.partitionBy("move_id", "move_sku_id").orderBy(F.col("move_qty").desc())
w_move_id_city = Window.partitionBy("move_id", "city")

scoped = (
    df_warehouses
    .filter((F.col("opened_on") < F.lit("2025-07-03").cast("date")) & (F.col("city") == "Singapore"))
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        F.coalesce(F.col("city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.quarter(F.col("opened_on")).alias("opened_on_quarter"),
        F.initcap(F.trim(F.col("wh_code"))).alias("wh_code_clean"),
    )
)

scored = (
    scoped
    .select(
        "id",
        "city",
        "opened_on",
        "city_or_unknown",
        "opened_on_quarter",
        "wh_code_clean",
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
        F.when(F.col("city_or_unknown").isin("Singapore", "Bangkok"), "group_a").when(F.col("city_or_unknown") == "Kuala Lumpur", "group_b").otherwise("other").alias("city_or_unknown_group"),
    )
)

exploded_detail = (
    scored.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.city",
        "s.opened_on",
        "s.opened_on_quarter",
        "s.wh_code_clean",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.operator").alias("move_operator"),
    )
)

ranked = (
    exploded_detail
    .select(
        "id",
        "city",
        "opened_on_quarter",
        "wh_code_clean",
        "move_id",
        "move_qty",
        "move_sku_id",
        "move_moved_at",
        "move_operator",
        F.max("opened_on_quarter").over(w_all_by_opened_on_quarter_run).alias("max_opened_on_quarter_so_far"),
        F.min("opened_on_quarter").over(w_move_id_move_operator).alias("move_id_move_operator_min_opened_on_quarter"),
        F.max("opened_on_quarter").over(w_all_by_opened_on_quarter_pri).alias("opened_on_quarter_max_3_prior"),
        F.first("move_qty").over(w_all_by_opened_on_quarter_ful).alias("first_move_qty"),
        F.max("opened_on_quarter").over(w_move_id_move_operator).alias("move_id_move_operator_max_opened_on_quarter"),
        F.row_number().over(w_all_by_opened_on_quarter).alias("rn"),
    )
)

windowed = (
    ranked
    .select(
        "id",
        "city",
        "opened_on_quarter",
        "move_id",
        "move_sku_id",
        "max_opened_on_quarter_so_far",
        "move_id_move_operator_min_opened_on_quarter",
        "opened_on_quarter_max_3_prior",
        "first_move_qty",
        "move_id_move_operator_max_opened_on_quarter",
        F.dense_rank().over(w_move_id_move_sku_id_by_move).alias("move_qty_dense_rank"),
        F.round(F.col("opened_on_quarter") / F.nullif(F.sum("opened_on_quarter").over(w_move_id_city), F.lit(0)), 4).alias("opened_on_quarter_share_of_move_id_city"),
        F.rank().over(w_move_id_move_sku_id_by_move).alias("move_qty_rank"),
    )
)

result = (
    windowed
    .rollup("move_qty_rank", "move_id", "move_sku_id")
    .agg(
        F.sum("max_opened_on_quarter_so_far").alias("total_max_opened_on_quarter_so_far"),
        F.countDistinct("city").alias("distinct_citys"),
    )
)
