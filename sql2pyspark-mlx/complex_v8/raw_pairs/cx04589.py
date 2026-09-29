from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_by_opened_on_full = Window.partitionBy("city").orderBy(F.col("opened_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_city = Window.partitionBy("city")
w_city_by_opened_on_running = Window.partitionBy("city").orderBy(F.col("opened_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_on_year_by_opened_on = Window.partitionBy(F.year("opened_on")).orderBy("opened_on", "id")
w_city_by_opened_on_ties = Window.partitionBy("city").orderBy(F.col("opened_on").desc())
w_city_seq_no = Window.partitionBy("city", "seq_no")
w_all_by_last_known_capacity = Window.orderBy(F.col("last_known_capacity").desc(), "id")

filtered_warehouses = (
    df_warehouses
    .filter((F.col("wh_code").isNotNull()) & (F.col("opened_on") < F.lit("2025-02-14").cast("date")))
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
    )
)

ranked = (
    filtered_warehouses
    .select(
        "id",
        "city",
        "capacity",
        F.first("opened_on").over(w_city_by_opened_on_full).alias("first_opened_on"),
        F.count(F.lit(1)).over(w_city).alias("rows_in_city"),
        F.last("capacity", ignorenulls=True).over(w_city_by_opened_on_running).alias("last_known_capacity"),
        F.row_number().over(w_opened_on_year_by_opened_on).alias("seq_no"),
        F.round(F.cume_dist().over(w_city_by_opened_on_ties), 3).alias("opened_on_cume_dist"),
    )
)

windowed = (
    ranked
    .select(
        "id",
        "city",
        "capacity",
        "rows_in_city",
        "seq_no",
        "opened_on_cume_dist",
        F.round(F.col("last_known_capacity") / F.nullif(F.sum("last_known_capacity").over(w_city_seq_no), F.lit(0)), 4).alias("last_known_capacity_share_of_city_seq_no"),
        F.sum("rows_in_city").over(w_city_seq_no).alias("city_seq_no_sum_rows_in_city"),
        F.row_number().over(w_all_by_last_known_capacity).alias("last_known_capacity_seq"),
        F.round(F.cume_dist().over(w_all_by_last_known_capacity), 3).alias("last_known_capacity_cume_dist"),
        F.lag("opened_on_cume_dist", 2).over(w_all_by_last_known_capacity).alias("prev2_opened_on_cume_dist"),
    )
)

qualified = (
    windowed.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.moved_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("last_known_capacity_cume_dist") <= 0.8)
)

result = (
    qualified
    .select(
        "id",
        F.col("opened_on_cume_dist").alias("opened_on_cume_dist_v"),
        F.col("last_known_capacity_cume_dist").alias("m_last_known_capacity_cume_dist"),
        F.col("last_known_capacity_share_of_city_seq_no").alias("last_known_capacity_share_of_city_seq_no_v"),
        F.col("prev2_opened_on_cume_dist").alias("m_prev2_opened_on_cume_dist"),
    )
    .unpivot(
        ids=["id"],
        values=["opened_on_cume_dist_v", "m_last_known_capacity_cume_dist", "last_known_capacity_share_of_city_seq_no_v", "m_prev2_opened_on_cume_dist"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
