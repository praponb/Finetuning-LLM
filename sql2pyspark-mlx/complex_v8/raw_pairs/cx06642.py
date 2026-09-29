from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country = Window.partitionBy("country")
w_country_by_birth_year_prior = Window.partitionBy("country").orderBy(F.col("birth_year").desc(), "id").rowsBetween(-3, -1)
w_country_by_birth_year_moving = Window.partitionBy("country").orderBy(F.col("birth_year").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_country_by_birth_year = Window.partitionBy("country").orderBy(F.col("birth_year").desc(), "id")

filtered = (
    df_listeners
    .filter((F.col("birth_year") <= 1990) & (~F.col("plan").isin("student", "premium")))
    .select(
        "id",
        "username",
        "country",
        "birth_year",
        "interests",
        F.substring(F.trim(F.col("username")), 1, 4).alias("username_prefix"),
    )
)

with_activity = (
    filtered
    .crossJoin(filtered.agg(F.min("birth_year").alias("overall_min_birth_year")))
    .filter(F.col("birth_year") > F.col("overall_min_birth_year"))
    .drop("overall_min_birth_year")
)

windowed = (
    with_activity
    .select(
        "*",
        F.sum("birth_year").over(w_country_by_birth_year_prior).alias("birth_year_sum_3_prior"),
        F.sum("birth_year").over(w_country_by_birth_year_moving).alias("birth_year_sum_2"),
        F.min("birth_year").over(w_country).alias("country_min_birth_year"),
        F.lead("birth_year").over(w_country_by_birth_year).alias("next_birth_year"),
        F.count(F.lit(1)).over(w_country).alias("rows_in_country"),
    )
)

result = (
    windowed
    .select(
        "id",
        "country",
        "birth_year",
        "username_prefix",
        "birth_year_sum_3_prior",
        "country_min_birth_year",
        "rows_in_country",
        F.length(F.trim(F.col("username_prefix"))).alias("username_prefix_len"),
    )
    .orderBy(F.col("username_prefix").desc(), F.col("username_prefix_len").desc())
)
