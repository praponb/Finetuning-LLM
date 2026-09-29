from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country = Window.partitionBy("country")
w_country_signup_date_month_by = Window.partitionBy("country", F.month("signup_date")).orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_country_signup_date_month_by_2 = Window.partitionBy("country", F.month("signup_date")).orderBy("signup_date", "id")
w_signup_date_month_plan_by_bi = Window.partitionBy(F.date_trunc("month", "signup_date"), "plan").orderBy("birth_year_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_country_signup_date_month_by_3 = Window.partitionBy("country", F.month("signup_date")).orderBy(F.col("birth_year").desc_nulls_first(), "id")

filtered = (
    df_listeners
    .filter((F.col("birth_year") >= 1969) | (F.col("country").isin("UK", "TH", "BR")))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.coalesce(F.col("birth_year"), F.lit(0)).alias("birth_year_filled"),
    )
)

windowed = (
    filtered
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        F.count(F.lit(1)).over(w_country).alias("rows_in_country"),
        F.min("birth_year").over(w_country_signup_date_month_by).alias("min_birth_year_so_far"),
        (F.col("birth_year_filled") - F.lag("birth_year_filled").over(w_country_signup_date_month_by_2)).alias("birth_year_filled_change"),
        F.first("signup_date", ignorenulls=True).over(w_signup_date_month_plan_by_bi).alias("first_known_signup_date"),
        F.max("birth_year_filled").over(w_country).alias("country_max_birth_year_filled"),
        F.round(F.col("birth_year") / F.nullif(F.sum("birth_year").over(w_country), F.lit(0)), 4).alias("birth_year_share_of_country"),
        F.round(F.avg("birth_year_filled").over(w_country), 2).alias("country_avg_birth_year_filled"),
        F.round(F.cume_dist().over(w_country_signup_date_month_by_3), 3).alias("birth_year_cume_dist"),
    )
)

result = (
    windowed
    .select(
        "id",
        "username",
        "signup_date",
        "birth_year",
        "rows_in_country",
        "min_birth_year_so_far",
        "first_known_signup_date",
        "country_max_birth_year_filled",
        "birth_year_share_of_country",
        "country_avg_birth_year_filled",
        "birth_year_cume_dist",
        F.greatest(F.col("birth_year_filled_change"), F.col("birth_year_cume_dist")).alias("greatest_birth_year_filled_change_birth_year_cum"),
    )
    .orderBy(F.col("min_birth_year_so_far").asc(), "rows_in_country", "id")
    .limit(20)
)
