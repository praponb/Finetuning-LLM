from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_interests_count_by_signup_da = Window.partitionBy("interests_count").orderBy("signup_date", "id").rowsBetween(-3, -1)
w_country_len_signup_date_year = Window.partitionBy("country_len", F.year("signup_date"))
w_interests_count_country_len = Window.partitionBy("interests_count", "country_len").orderBy("interests_count", "id")
w_country_interests_count = Window.partitionBy("country", "interests_count")
w_all_by_interests_count_full = Window.orderBy("interests_count", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_listeners = (
    df_listeners
    .filter(F.col("country") == "JP")
    .select(
        "id",
        "country",
        "signup_date",
        "interests",
        F.size(F.col("interests")).alias("interests_count"),
        F.length(F.trim(F.col("country"))).alias("country_len"),
    )
)

metrics = (
    filtered_listeners
    .select(
        "*",
        F.round(F.avg("country_len").over(w_interests_count_by_signup_da), 2).alias("country_len_avg_3_prior"),
        F.sum("country_len").over(w_country_len_signup_date_year).alias("country_len_signup_date_year_sum_country_len"),
        F.round(F.percent_rank().over(w_interests_count_country_len), 3).alias("interests_count_percent_rank"),
    )
)

with_trends = (
    metrics
    .select(
        "id",
        "country",
        "interests_count",
        "country_len",
        "country_len_avg_3_prior",
        "country_len_signup_date_year_sum_country_len",
        "interests_count_percent_rank",
        F.count(F.lit(1)).over(w_country_interests_count).alias("rows_in_country_interests_count"),
        F.last("country_len").over(w_all_by_interests_count_full).alias("last_country_len"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "country",
        "country_len_avg_3_prior",
        "interests_count_percent_rank",
        "rows_in_country_interests_count",
        "last_country_len",
    )
    .orderBy(
        F.col("rows_in_country_interests_count").asc_nulls_last(),
        F.col("country").desc(),
    )
)
