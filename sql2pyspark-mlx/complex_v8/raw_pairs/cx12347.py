from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_plan_by_avg_play_ms = Window.partitionBy("country", "plan").orderBy("avg_play_ms_played_2", "id")
w_plan_group_by_birth_year = Window.partitionBy("plan_group").orderBy(F.col("birth_year").desc(), "id")
w_country_plan_by_avg_play_ms_2 = Window.partitionBy("country", "plan").orderBy("avg_play_ms_played_2", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_country = Window.partitionBy("country")
w_avg_play_ms_played_2_quartil = Window.partitionBy("avg_play_ms_played_2_quartile", "birth_year_quintile").orderBy(F.col("second_avg_play_ms_played").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_avg_play_ms_played_2_quartil_2 = Window.partitionBy("avg_play_ms_played_2_quartile", "birth_year_quintile").orderBy(F.col("second_avg_play_ms_played").asc_nulls_last(), "id")
w_all_by_signup_date = Window.orderBy("signup_date", "id")

prepared_listeners = (
    df_listeners
    .filter((F.col("username").isNotNull()) | (F.year(F.col("signup_date")) == 2025))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.when(F.col("plan").isin("student", "free"), "group_a").when(F.col("plan") == "premium", "group_b").otherwise("other").alias("plan_group"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played_2"),
    )
)

with_counts = (
    prepared_listeners.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select("s.*", "p.avg_play_ms_played", "p.avg_play_ms_played_2")
)

windowed = (
    with_counts
    .select(
        "id",
        "username",
        "plan",
        "signup_date",
        "birth_year",
        "interests",
        "avg_play_ms_played",
        "avg_play_ms_played_2",
        F.ntile(4).over(w_country_plan_by_avg_play_ms).alias("avg_play_ms_played_2_quartile"),
        F.ntile(5).over(w_plan_group_by_birth_year).alias("birth_year_quintile"),
        F.nth_value("avg_play_ms_played", 2).over(w_country_plan_by_avg_play_ms_2).alias("second_avg_play_ms_played"),
        F.size(F.collect_set("plan").over(w_country)).alias("distinct_plan_in_country"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.first("signup_date", ignorenulls=True).over(w_avg_play_ms_played_2_quartil).alias("first_known_signup_date"),
        F.ntile(4).over(w_avg_play_ms_played_2_quartil_2).alias("second_avg_play_ms_played_quartile"),
        F.ntile(10).over(w_all_by_signup_date).alias("signup_date_decile"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["birth_year_quintile", "plan", "signup_date_decile"], ["birth_year_quintile"], []],
        "birth_year_quintile", "plan", "signup_date_decile",
    )
    .agg(
        F.grouping("birth_year_quintile").alias("is_total_birth_year_quintile"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("birth_year_quintile").asc_nulls_last(),
        F.col("plan").asc_nulls_last(),
        F.col("signup_date_decile").asc_nulls_last(),
    )
)
