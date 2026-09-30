from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city = Window.partitionBy("city")
w_city_by_capacity = Window.partitionBy("city").orderBy("capacity", "id")
w_city_by_capacity_running = Window.partitionBy("city").orderBy("capacity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_city_by_capacity_prior_1 = Window.partitionBy("city").orderBy("capacity", "id").rowsBetween(-1, -1)
w_capacity_rank_by_capacity_ma = Window.partitionBy("capacity_rank").orderBy("capacity_max_1_prior", "id")
w_capacity_rank = Window.partitionBy("capacity_rank")
w_capacity_rank_by_capacity_ma_2 = Window.partitionBy("capacity_rank").orderBy("capacity_max_1_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_capacity_max_1_prior_decile = Window.partitionBy("capacity_max_1_prior_decile").orderBy(F.col("capacity_share_of_city").asc_nulls_last(), "id")
w_capacity_max_1_prior_decile_2 = Window.partitionBy("capacity_max_1_prior_decile")

cleaned = (
    df_warehouses
    .filter(F.upper(F.trim(F.col("wh_code"))).like("B%"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        F.add_months(F.col("opened_on"), 1).alias("opened_on_plus_1m"),
        F.concat_ws(" / ", F.col("wh_code"), F.col("city")).alias("wh_code_city_label"),
        F.concat_ws("|", F.col("wh_code"), F.col("city")).alias("wh_code_city_label_2"),
    )
)

qualified = (
    cleaned
    .crossJoin(cleaned.agg(F.avg("capacity").alias("overall_avg_capacity")))
    .filter(F.col("capacity") > F.col("overall_avg_capacity"))
    .drop("overall_avg_capacity")
)

windowed = (
    qualified
    .select(
        "id",
        "opened_on_plus_1m",
        "wh_code_city_label",
        "wh_code_city_label_2",
        F.sum("capacity").over(w_city).alias("city_sum_capacity"),
        F.rank().over(w_city_by_capacity).alias("capacity_rank"),
        F.min("capacity").over(w_city_by_capacity_running).alias("min_capacity_so_far"),
        F.max("capacity").over(w_city_by_capacity_prior_1).alias("capacity_max_1_prior"),
        F.round(F.col("capacity") / F.nullif(F.sum("capacity").over(w_city), F.lit(0)), 4).alias("capacity_share_of_city"),
        F.first("capacity", ignorenulls=True).over(w_city_by_capacity_running).alias("first_known_capacity"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.lead("first_known_capacity").over(w_capacity_rank_by_capacity_ma).alias("next_first_known_capacity"),
        F.ntile(10).over(w_capacity_rank_by_capacity_ma).alias("capacity_max_1_prior_decile"),
        F.dense_rank().over(w_capacity_rank_by_capacity_ma).alias("capacity_max_1_prior_dense_rank"),
        F.sum("first_known_capacity").over(w_capacity_rank_by_capacity_ma_2).alias("running_first_known_capacity"),
        F.min("first_known_capacity").over(w_capacity_rank).alias("capacity_rank_min_first_known_capacity"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "opened_on_plus_1m",
        "wh_code_city_label",
        "city_sum_capacity",
        "capacity_rank",
        "capacity_max_1_prior",
        "capacity_share_of_city",
        "first_known_capacity",
        "next_first_known_capacity",
        "capacity_max_1_prior_decile",
        "capacity_max_1_prior_dense_rank",
        "running_first_known_capacity",
        "capacity_rank_min_first_known_capacity",
        F.lag("min_capacity_so_far", 3).over(w_capacity_max_1_prior_decile).alias("prev3_min_capacity_so_far"),
        F.count(F.lit(1)).over(w_capacity_max_1_prior_decile_2).alias("rows_in_capacity_max_1_prior_decile"),
        F.sum("city_sum_capacity").over(w_capacity_max_1_prior_decile_2).alias("capacity_max_1_prior_decile_sum_city_sum_capacit"),
        F.rank().over(w_capacity_max_1_prior_decile).alias("capacity_share_of_city_rank"),
    )
)

enriched = (
    sequenced_2
    .select(
        "id",
        "opened_on_plus_1m",
        "wh_code_city_label",
        "city_sum_capacity",
        "capacity_max_1_prior",
        "capacity_share_of_city",
        "first_known_capacity",
        "next_first_known_capacity",
        "running_first_known_capacity",
        "rows_in_capacity_max_1_prior_decile",
        "capacity_max_1_prior_decile_sum_city_sum_capacit",
        "capacity_share_of_city_rank",
        F.least(F.col("city_sum_capacity"), F.col("capacity_share_of_city")).alias("least_city_sum_capacity_capacity_share_of_city"),
    )
)

filtered = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.qty") < 415), "left_semi")
    .filter(F.col("running_first_known_capacity") > 30850)
)

result = (
    filtered
    .select(
        "id",
        "least_city_sum_capacity_capacity_share_of_city",
        "capacity_share_of_city",
    )
    .unpivot(
        ids=["id"],
        values=["least_city_sum_capacity_capacity_share_of_city", "capacity_share_of_city"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
