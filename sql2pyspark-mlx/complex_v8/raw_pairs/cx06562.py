from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city = Window.partitionBy("city")
w_opened_on_quarter_city_by_op = Window.partitionBy("opened_on_quarter", "city").orderBy("opened_on_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_on_quarter_city_by_op_2 = Window.partitionBy("opened_on_quarter", "city").orderBy("opened_on_quarter", "id")
w_opened_on_quarter = Window.partitionBy("opened_on_quarter")
w_all_by_opened_on_quarter_min = Window.orderBy("opened_on_quarter_minus_capacity", "id").rowsBetween(-6, -1)

cleaned = (
    df_warehouses
    .filter((~F.col("city").isin("Singapore", "Bangkok")) & (F.col("opened_on") < F.lit("2025-07-03").cast("date")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        F.quarter(F.col("opened_on")).alias("opened_on_quarter"),
    )
)

screened = (
    cleaned.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.unit_price") < 289.7) & (F.col("st.move_type") == "OUTBOUND"), "left_semi")
    .filter(F.col("city") == "Bangkok")
)

enriched = (
    screened
    .filter(F.col("city") == "Singapore")
    .select(
        "*",
        F.concat_ws("|", F.col("city"), F.col("wh_code")).alias("city_wh_code_label"),
        (F.col("opened_on_quarter") - F.col("capacity")).alias("opened_on_quarter_minus_capacity"),
    )
)

windowed = (
    enriched
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on_quarter",
        "opened_on_quarter_minus_capacity",
        F.count(F.lit(1)).over(w_city).alias("rows_in_city"),
        F.round(F.col("opened_on_quarter") - F.avg("opened_on_quarter").over(w_city), 2).alias("opened_on_quarter_vs_city_avg"),
        F.min("opened_on_quarter").over(w_opened_on_quarter_city_by_op).alias("min_opened_on_quarter_so_far"),
        F.lag("capacity", 3, 0).over(w_opened_on_quarter_city_by_op_2).alias("prev3_capacity"),
    )
)

with_windows = (
    windowed
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on_quarter_minus_capacity",
        "rows_in_city",
        "min_opened_on_quarter_so_far",
        "prev3_capacity",
        F.round(F.col("opened_on_quarter") / F.nullif(F.sum("opened_on_quarter").over(w_opened_on_quarter), F.lit(0)), 4).alias("opened_on_quarter_share_of_opened_on_quarter"),
        F.sum("prev3_capacity").over(w_all_by_opened_on_quarter_min).alias("prev3_capacity_sum_6_prior"),
    )
)

result = (
    with_windows
    .filter(F.col("wh_code").isNotNull())
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on_quarter_minus_capacity",
        "rows_in_city",
        "min_opened_on_quarter_so_far",
        "opened_on_quarter_share_of_opened_on_quarter",
        "prev3_capacity_sum_6_prior",
        F.concat(F.col("wh_code"), F.lit("#"), F.col("prev3_capacity").cast("string")).alias("wh_code_ref"),
    )
    .orderBy(
        F.col("wh_code_ref").desc_nulls_first(),
        F.col("min_opened_on_quarter_so_far").desc_nulls_first(),
    )
)
