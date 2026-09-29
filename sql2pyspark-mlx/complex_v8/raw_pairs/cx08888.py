from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_hired_on_full = Window.orderBy(F.col("hired_on").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_hired_on_full_2 = Window.orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_hired_on = Window.orderBy(F.col("hired_on").desc_nulls_first(), "id")
w_all_by_hired_on_ties = Window.orderBy(F.col("hired_on").desc())
w_all_by_hired_on_2 = Window.orderBy(F.col("hired_on").desc(), "id")
w_team_by_hired_on = Window.partitionBy("team").orderBy(F.col("hired_on").desc(), "id")
w_all_by_hired_on_3 = Window.orderBy("hired_on", "id")
w_all_by_hired_on_ties_2 = Window.orderBy(F.col("hired_on").desc_nulls_first())
w_all_by_hired_on_running = Window.orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_hired_on_ties_3 = Window.orderBy("hired_on")
w_hired_on_quintile_by_hired_o = Window.partitionBy("hired_on_quintile").orderBy("hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hired_on_quintile_by_hired_o_2 = Window.partitionBy("hired_on_quintile").orderBy("hired_on")
w_prev_hired_on_year = Window.partitionBy(F.year("prev_hired_on"))
w_hired_on_quintile_by_prev_hi = Window.partitionBy("hired_on_quintile").orderBy(F.col("prev_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_agents = (
    df_support_agents
    .filter((F.col("agent_name").isNotNull()) & (F.col("hired_on") >= F.lit("2024-05-02").cast("date")))
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
    )
)

ranked = (
    recent_agents
    .select(
        "id",
        "team",
        "hired_on",
        F.nth_value("hired_on", 2).over(w_all_by_hired_on_full).alias("second_hired_on"),
        F.first("hired_on").over(w_all_by_hired_on_full_2).alias("first_hired_on"),
        F.datediff(F.col("hired_on"), F.lag("hired_on").over(w_all_by_hired_on)).alias("days_since_prev_hired_on"),
        F.round(F.cume_dist().over(w_all_by_hired_on_ties), 3).alias("hired_on_cume_dist"),
        F.lag("hired_on").over(w_all_by_hired_on_2).alias("prev_hired_on"),
        F.lead("hired_on", 3).over(w_team_by_hired_on).alias("next3_hired_on"),
        F.ntile(5).over(w_all_by_hired_on_3).alias("hired_on_quintile"),
        F.round(F.percent_rank().over(w_team_by_hired_on), 3).alias("hired_on_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_hired_on_ties_2), 3).alias("hired_on_cume_dist_2"),
        F.row_number().over(w_all_by_hired_on).alias("hired_on_seq"),
        F.first("hired_on", ignorenulls=True).over(w_all_by_hired_on_running).alias("first_known_hired_on"),
        F.dense_rank().over(w_all_by_hired_on_ties_3).alias("hired_on_dense_rank"),
    )
)

ranked_2 = (
    ranked
    .select(
        "*",
        F.last("hired_on_cume_dist", ignorenulls=True).over(w_hired_on_quintile_by_hired_o).alias("last_known_hired_on_cume_dist"),
        F.round(F.cume_dist().over(w_hired_on_quintile_by_hired_o_2), 3).alias("hired_on_cume_dist_3"),
        F.count(F.lit(1)).over(w_prev_hired_on_year).alias("rows_in_prev_hired_on_year"),
        F.nth_value("next3_hired_on", 2).over(w_hired_on_quintile_by_prev_hi).alias("second_next3_hired_on"),
    )
)

result = (
    ranked_2
    .filter(F.col("rows_in_prev_hired_on_year") >= 3)
    .select(
        "id",
        "team",
        "hired_on",
        "second_hired_on",
        "days_since_prev_hired_on",
        "hired_on_cume_dist",
        "prev_hired_on",
        "hired_on_quintile",
        "hired_on_percent_rank",
        "hired_on_cume_dist_2",
        "hired_on_seq",
        "hired_on_dense_rank",
        "hired_on_cume_dist_3",
        "rows_in_prev_hired_on_year",
        F.concat(F.col("team"), F.lit("#"), F.col("days_since_prev_hired_on").cast("string")).alias("team_ref"),
        F.greatest(F.col("hired_on_dense_rank"), F.col("hired_on_quintile")).alias("greatest_hired_on_dense_rank_hired_on_quintile"),
    )
)
