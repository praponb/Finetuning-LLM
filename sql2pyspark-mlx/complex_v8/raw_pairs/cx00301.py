from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_year = Window.partitionBy(F.year("moved_at"))
w_warehouse_id_moved_at_month = Window.partitionBy("warehouse_id", F.month("moved_at")).orderBy(F.col("unit_price").desc_nulls_first(), "id")
w_warehouse_id_moved_at_month_2 = Window.partitionBy("warehouse_id", F.month("moved_at")).orderBy(F.col("unit_price").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_to_date_by_moved_at = Window.partitionBy(F.to_date("moved_at")).orderBy(F.col("moved_at").desc())
w_warehouse_id_by_rows_in_move = Window.partitionBy("warehouse_id").orderBy("rows_in_moved_at_year", "id").rowsBetween(-1, -1)
w_all_by_minutes_since_prev_mo = Window.orderBy(F.col("minutes_since_prev_moved_at").desc(), "id")
w_distinct_operator_in_moved_a = Window.partitionBy("distinct_operator_in_moved_at_year").orderBy("moved_at_year_min_unit_price")
w_moved_at_month = Window.partitionBy(F.date_trunc("month", "moved_at"))
w_distinct_operator_in_moved_a_2 = Window.partitionBy("distinct_operator_in_moved_at_year").orderBy("moved_at_year_min_unit_price", "id")
w_warehouse_id_by_rows_in_move_2 = Window.partitionBy("warehouse_id").orderBy("rows_in_moved_at_year", "id")

cleaned_moves = (
    df_stock_moves
    .filter(F.col("qty") > 93)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
    )
)

ranked = (
    cleaned_moves
    .select(
        "*",
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_moved_at_year), 2).alias("unit_price_vs_moved_at_year_avg"),
        F.dense_rank().over(w_warehouse_id_moved_at_month).alias("unit_price_dense_rank"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_warehouse_id_moved_at_month))) / 60, 1).alias("minutes_since_prev_moved_at"),
        F.max("unit_price").over(w_moved_at_year).alias("moved_at_year_max_unit_price"),
        F.size(F.collect_set("operator").over(w_moved_at_year)).alias("distinct_operator_in_moved_at_year"),
        F.first("unit_price", ignorenulls=True).over(w_warehouse_id_moved_at_month_2).alias("first_known_unit_price"),
        F.rank().over(w_moved_at_to_date_by_moved_at).alias("moved_at_rank"),
        F.lag("moved_at").over(w_warehouse_id_moved_at_month).alias("prev_moved_at"),
        F.min("unit_price").over(w_moved_at_year).alias("moved_at_year_min_unit_price"),
        F.count(F.lit(1)).over(w_moved_at_year).alias("rows_in_moved_at_year"),
    )
)

first_rows = (
    ranked
    .filter(F.col("unit_price_dense_rank") <= 2)
    .drop("unit_price_dense_rank")
)

with_trends = (
    first_rows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price_vs_moved_at_year_avg",
        "minutes_since_prev_moved_at",
        "distinct_operator_in_moved_at_year",
        "first_known_unit_price",
        "moved_at_rank",
        "rows_in_moved_at_year",
        F.max("minutes_since_prev_moved_at").over(w_warehouse_id_by_rows_in_move).alias("minutes_since_prev_moved_at_max_1_prior"),
        F.lag("prev_moved_at").over(w_all_by_minutes_since_prev_mo).alias("prev_prev_moved_at"),
        F.rank().over(w_distinct_operator_in_moved_a).alias("moved_at_year_min_unit_price_rank"),
        F.round(F.col("minutes_since_prev_moved_at") - F.avg("minutes_since_prev_moved_at").over(w_moved_at_month), 2).alias("minutes_since_prev_moved_at_vs_moved_at_month_av"),
        F.round((F.unix_timestamp("prev_moved_at") - F.unix_timestamp(F.lag("prev_moved_at").over(w_distinct_operator_in_moved_a_2))) / 60, 1).alias("minutes_since_prev_prev_moved_at"),
        F.lead("moved_at_year_max_unit_price").over(w_distinct_operator_in_moved_a_2).alias("next_moved_at_year_max_unit_price"),
        F.ntile(5).over(w_warehouse_id_by_rows_in_move_2).alias("rows_in_moved_at_year_quintile"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price_vs_moved_at_year_avg",
        "minutes_since_prev_moved_at",
        "distinct_operator_in_moved_at_year",
        "moved_at_rank",
        "minutes_since_prev_moved_at_max_1_prior",
        "moved_at_year_min_unit_price_rank",
        "minutes_since_prev_moved_at_vs_moved_at_month_av",
        "minutes_since_prev_prev_moved_at",
        "next_moved_at_year_max_unit_price",
        "rows_in_moved_at_year_quintile",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
        F.dayofweek(F.col("prev_prev_moved_at")).alias("prev_prev_moved_at_dayofweek"),
    )
    .orderBy(
        F.col("distinct_operator_in_moved_at_year").desc_nulls_first(),
        F.col("rows_in_moved_at_year_quintile").desc(),
    )
)
