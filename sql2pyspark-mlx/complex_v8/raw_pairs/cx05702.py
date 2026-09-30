from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt_first_signup_date_month = Window.partitionBy("cnt", F.date_trunc("month", "first_signup_date")).orderBy(F.col("total_birth_year").desc_nulls_first(), "signup_date_ym")

base = (
    df_listeners
    .filter((F.col("birth_year") <= 1994) & (F.col("country") != "JP"))
    .select(
        "id",
        "username",
        "signup_date",
        "birth_year",
        "interests",
        F.date_format(F.col("signup_date"), "yyyy-MM-dd").alias("signup_date_label"),
        F.when(F.col("birth_year") >= 1981, "HIGH").when(F.col("birth_year") >= 1975, "MEDIUM").otherwise("LOW").alias("birth_year_tier"),
    )
)

stats = (
    base
    .filter(F.col("signup_date_label").isNotNull())
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_date_ym"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum("birth_year").alias("total_birth_year"),
        F.sum(F.when(F.upper(F.trim(F.col("username"))).like("C%"), F.col("birth_year"))).alias("birth_year_upper_trim_username_like_c"),
        F.min("signup_date").alias("first_signup_date"),
    )
)

windowed = (
    stats
    .select(
        "signup_date_ym",
        "cnt",
        "total_birth_year",
        "birth_year_upper_trim_username_like_c",
        F.lead("total_birth_year", 2, 0).over(w_cnt_first_signup_date_month).alias("next2_total_birth_year"),
        F.row_number().over(w_cnt_first_signup_date_month).alias("rn"),
    )
)

scored = (
    windowed
    .filter(F.col("rn") >= 3)
    .select(
        "*",
        F.round(F.col("total_birth_year") / F.nullif(F.col("rn"), F.lit(0)), 3).alias("total_birth_year_per_rn"),
        F.least(F.col("total_birth_year"), F.col("next2_total_birth_year")).alias("least_total_birth_year_next2_total_birth_year"),
        F.substring(F.trim(F.col("signup_date_ym")), 1, 2).alias("signup_date_ym_prefix"),
    )
)

result = (
    scored
    .groupingSets(
        [["cnt", "rn"], ["cnt"], ["rn"], []],
        "cnt", "rn",
    )
    .agg(
        F.min("least_total_birth_year_next2_total_birth_year").alias("min_least_total_birth_year_next2_total_birth_yea"),
    )
    .orderBy(F.col("cnt").asc_nulls_last(), F.col("rn").asc_nulls_last())
)
