from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country = Window.partitionBy("country")
w_plan_or_unknown_by_birth_yea = Window.partitionBy("plan_or_unknown").orderBy("birth_year", "id")
w_country_plan_by_birth_year_f = Window.partitionBy("country", "plan").orderBy("birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_country_plan_by_birth_year_r = Window.partitionBy("country", "plan").orderBy("birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_listeners = (
    df_listeners
    .filter(F.col("signup_date").between(F.lit("2024-07-13").cast("date"), F.lit("2025-05-05").cast("date")))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "birth_year",
        "interests",
        F.coalesce(F.col("plan"), F.lit("UNKNOWN")).alias("plan_or_unknown"),
        F.substring(F.trim(F.col("country")), 1, 3).alias("country_prefix"),
        F.abs(F.col("birth_year") - 1981).alias("birth_year_distance"),
    )
)

metrics = (
    cleaned_listeners
    .select(
        "id",
        "username",
        "plan",
        "country",
        "birth_year",
        "interests",
        "plan_or_unknown",
        "country_prefix",
        "birth_year_distance",
        F.count(F.lit(1)).over(w_country).alias("rows_in_country"),
        (F.col("birth_year_distance") - F.lag("birth_year_distance").over(w_plan_or_unknown_by_birth_yea)).alias("birth_year_distance_change"),
        F.nth_value("birth_year_distance", 2).over(w_country_plan_by_birth_year_f).alias("second_birth_year_distance"),
        F.first("birth_year", ignorenulls=True).over(w_country_plan_by_birth_year_r).alias("first_known_birth_year"),
    )
)

filtered = (
    metrics.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.month(F.col("p.played_at")).isin(1, 2)) & (F.col("p.source") == "radio"), "left_semi")
)

result = (
    filtered
    .cube("plan", "country")
    .agg(
        F.count(F.when(F.col("username").isNotNull(), 1)).alias("n_username_is_not_null"),
        F.countDistinct("plan_or_unknown").alias("distinct_plan_or_unknowns"),
    )
    .select(
        F.coalesce(F.col("plan"), F.lit("ALL")).alias("plan"),
        F.coalesce(F.col("country"), F.lit("ALL")).alias("country"),
        "n_username_is_not_null",
        "distinct_plan_or_unknowns",
    )
)
