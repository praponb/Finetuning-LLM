from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_by_capacity_ties = Window.partitionBy("city").orderBy("capacity")
w_city_by_opened_on_full = Window.partitionBy("city").orderBy("opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_city_by_opened_on_ties = Window.partitionBy("city").orderBy("opened_on")
w_city_by_opened_on = Window.partitionBy("city").orderBy("opened_on", "id")
w_city_by_capacity_moving_6 = Window.partitionBy("city").orderBy("capacity", "id").rowsBetween(-6, Window.currentRow)

base_warehouses = (
    df_warehouses
    .filter(F.col("city") != "Kuala Lumpur")
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
    )
)

tagged = (
    base_warehouses
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.date_format(F.col("opened_on"), "yyyy-MM").alias("opened_on_label"),
    )
)

sequenced = (
    tagged
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
        "opened_on_label",
        F.rank().over(w_city_by_capacity_ties).alias("capacity_rank"),
        F.first("capacity").over(w_city_by_opened_on_full).alias("first_capacity"),
        F.rank().over(w_city_by_opened_on_ties).alias("opened_on_rank"),
        F.rank().over(w_city_by_opened_on).alias("opened_on_rank_2"),
        F.round(F.avg("capacity").over(w_city_by_capacity_moving_6), 2).alias("capacity_avg_7"),
    )
)

leaders = sequenced.filter(F.col("capacity_rank") <= 3)

leaders_2 = leaders.filter((F.col("opened_on_rank_2") <= 3) & (F.col("opened_on_label").isNotNull()))

with_activity = (
    leaders_2.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.operator") == "op_eve") & (F.col("st.qty") <= 314), "left_semi")
    .filter(F.col("opened_on").between(F.lit("2024-02-19").cast("date"), F.lit("2025-12-17").cast("date")))
)

result = (
    with_activity
    .groupingSets(
        [["opened_on_rank_2", "opened_on_rank"], ["opened_on_rank_2"]],
        "opened_on_rank_2", "opened_on_rank",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("opened_on_rank").alias("total_opened_on_rank"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("opened_on_rank_2").asc_nulls_last(),
        F.col("opened_on_rank").asc_nulls_last(),
    )
)
