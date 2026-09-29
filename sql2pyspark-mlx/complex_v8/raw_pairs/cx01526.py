from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_plan_country_by_birth_year_f = Window.partitionBy("plan", "country").orderBy("birth_year_filled", "id").rowsBetween(-2, Window.currentRow)
w_all_by_birth_year_filled = Window.orderBy("birth_year_filled", "id")
w_all_by_birth_year_filled_pri = Window.orderBy("birth_year_filled", "id").rowsBetween(-1, -1)
w_plan_country_by_birth_year_f_2 = Window.partitionBy("plan", "country").orderBy("birth_year_filled", "id").rowsBetween(-1, -1)
w_plan_country_by_birth_year_f_3 = Window.partitionBy("plan", "country").orderBy("birth_year_filled", "id")
w_plan_country_by_birth_year_f_4 = Window.partitionBy("plan", "country").orderBy("birth_year_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_birth_year_filled_2 = Window.orderBy(F.col("birth_year_filled").desc_nulls_first(), "id")
w_plan = Window.partitionBy("plan")

cleaned_listeners = (
    df_listeners
    .filter((F.col("country") == "JP") | (F.array_contains(F.col("interests"), "indie")))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "interests",
        F.coalesce(F.col("birth_year"), F.lit(0)).alias("birth_year_filled"),
    )
)

sequenced = (
    cleaned_listeners
    .select(
        "*",
        F.max("birth_year_filled").over(w_plan_country_by_birth_year_f).alias("birth_year_filled_max_3"),
        F.row_number().over(w_all_by_birth_year_filled).alias("rn"),
        F.round(F.avg("birth_year_filled").over(w_all_by_birth_year_filled_pri), 2).alias("birth_year_filled_avg_1_prior"),
        F.round(F.avg("birth_year_filled").over(w_plan_country_by_birth_year_f_2), 2).alias("birth_year_filled_avg_1_prior_2"),
        (F.col("birth_year_filled") - F.lag("birth_year_filled").over(w_plan_country_by_birth_year_f_3)).alias("birth_year_filled_change"),
        F.last("signup_date", ignorenulls=True).over(w_plan_country_by_birth_year_f_4).alias("last_known_signup_date"),
        F.ntile(3).over(w_all_by_birth_year_filled_2).alias("birth_year_filled_tercile"),
        F.round(F.avg("birth_year_filled").over(w_plan), 2).alias("plan_avg_birth_year_filled"),
        F.size(F.collect_set("country").over(w_plan)).alias("distinct_country_in_plan"),
        F.round(F.col("birth_year_filled") / F.nullif(F.sum("birth_year_filled").over(w_plan), F.lit(0)), 4).alias("birth_year_filled_share_of_plan"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "plan",
        "country",
        "signup_date",
        "interests",
        "birth_year_filled",
        "birth_year_filled_max_3",
        "rn",
        "birth_year_filled_avg_1_prior",
        "birth_year_filled_avg_1_prior_2",
        "birth_year_filled_change",
        "birth_year_filled_tercile",
        "plan_avg_birth_year_filled",
        "distinct_country_in_plan",
        F.date_trunc("month", F.col("last_known_signup_date")).alias("last_known_signup_date_month"),
        F.datediff(F.col("signup_date"), F.col("last_known_signup_date")).alias("days_last_known_signup_date_to_signup_date"),
    )
    .orderBy("birth_year_filled_change", "rn", "id")
    .limit(20)
)
