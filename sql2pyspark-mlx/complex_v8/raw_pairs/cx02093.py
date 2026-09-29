from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_signup_date_quarter_by_birth = Window.partitionBy("signup_date_quarter").orderBy("birth_year_filled", "id")
w_signup_date_quarter_by_birth_2 = Window.partitionBy("signup_date_quarter").orderBy("birth_year_filled", "id").rowsBetween(-2, -1)
w_all_by_birth_year_filled_dis = Window.orderBy(F.col("birth_year_filled_distance").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_birth_year = Window.orderBy("birth_year", "id")
w_all_by_birth_year_prior_2 = Window.orderBy("birth_year", "id").rowsBetween(-2, -1)
w_all_by_birth_year_moving_3 = Window.orderBy("birth_year", "id").rowsBetween(-3, Window.currentRow)

valid_listeners = (
    df_listeners
    .filter((F.size(F.col("interests")) >= 1) & (F.col("birth_year").between(1968, 1981)))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.coalesce(F.col("birth_year"), F.lit(0)).alias("birth_year_filled"),
        F.abs(F.col("birth_year") - 1980).alias("birth_year_distance"),
        F.substring(F.trim(F.col("plan")), 1, 3).alias("plan_prefix"),
    )
)

derived = (
    valid_listeners
    .select(
        "id",
        "username",
        "birth_year",
        "interests",
        "birth_year_filled",
        "birth_year_distance",
        "plan_prefix",
        F.abs(F.col("birth_year_filled") - 1974).alias("birth_year_filled_distance"),
        F.concat(F.col("username"), F.lit("#"), F.col("birth_year_distance").cast("string")).alias("username_ref"),
        F.quarter(F.col("signup_date")).alias("signup_date_quarter"),
    )
)

windowed = (
    derived
    .select(
        "*",
        F.ntile(10).over(w_signup_date_quarter_by_birth).alias("birth_year_filled_decile"),
        F.max("birth_year_filled_distance").over(w_signup_date_quarter_by_birth_2).alias("birth_year_filled_distance_max_2_prior"),
    )
)

annotated = (
    windowed
    .select(
        "id",
        "birth_year",
        "interests",
        "birth_year_filled",
        "plan_prefix",
        "birth_year_filled_distance",
        "username_ref",
        "birth_year_filled_distance_max_2_prior",
        F.coalesce(F.col("birth_year_filled"), F.lit(0)).alias("birth_year_filled_filled"),
    )
)

windowed_2 = (
    annotated
    .select(
        "id",
        "birth_year",
        "interests",
        "birth_year_filled",
        "plan_prefix",
        "birth_year_filled_distance",
        "username_ref",
        "birth_year_filled_distance_max_2_prior",
        F.nth_value("birth_year", 2).over(w_all_by_birth_year_filled_dis).alias("second_birth_year"),
        F.ntile(3).over(w_all_by_birth_year).alias("birth_year_tercile"),
        F.lag("birth_year_filled_distance", 2).over(w_all_by_birth_year).alias("prev2_birth_year_filled_distance"),
        F.sum("birth_year_filled_distance").over(w_all_by_birth_year_prior_2).alias("birth_year_filled_distance_sum_2_prior"),
        (F.col("birth_year_filled_distance") - F.lag("birth_year_filled_distance").over(w_all_by_birth_year)).alias("birth_year_filled_distance_change"),
        F.round(F.avg("birth_year_filled").over(w_all_by_birth_year_moving_3), 2).alias("birth_year_filled_avg_4"),
    )
)

result = (
    windowed_2
    .filter(F.col("birth_year_filled") > 1970)
    .select(
        "id",
        "birth_year",
        "interests",
        "birth_year_filled",
        "plan_prefix",
        "birth_year_filled_distance_max_2_prior",
        "second_birth_year",
        "birth_year_tercile",
        "prev2_birth_year_filled_distance",
        "birth_year_filled_distance_change",
        F.least(F.col("birth_year_tercile"), F.col("birth_year")).alias("least_birth_year_tercile_birth_year"),
    )
    .orderBy("birth_year_filled_distance_change")
)
