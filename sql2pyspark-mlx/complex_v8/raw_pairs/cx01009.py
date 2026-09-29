from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_row_count_n_country_is_not_n = Window.partitionBy("row_count", "n_country_is_not_null_2").orderBy(F.col("total_birth_year").desc())
w_row_count_n_country_is_not_n_2 = Window.partitionBy("row_count", "n_country_is_not_null_2").orderBy(F.col("total_birth_year").desc(), "signup_date_ym").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_row_count_n_country_is_not_n_3 = Window.partitionBy("row_count", "n_country_is_not_null_2").orderBy(F.col("total_birth_year").desc(), "signup_date_ym")
w_row_count_n_country_is_not_n_4 = Window.partitionBy("row_count", "n_country_is_not_null_2").orderBy(F.col("total_birth_year").desc(), "signup_date_ym").rowsBetween(-1, Window.currentRow)

prepared_listeners = (
    df_listeners
    .filter(F.array_contains(F.col("interests"), "indie"))
    .select(
        "id",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.concat(F.col("username"), F.lit("#"), F.col("id").cast("string")).alias("username_ref"),
        F.initcap(F.trim(F.col("username"))).alias("username_clean"),
    )
)

grouped = (
    prepared_listeners
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_date_ym"))
    .agg(
        F.count("*").alias("row_count"),
        F.sum("birth_year").alias("total_birth_year"),
        F.count(F.when(F.col("country").isNotNull(), 1)).alias("n_country_is_not_null"),
        F.sum(F.when(F.col("country").isNotNull(), 1).otherwise(0)).alias("n_country_is_not_null_2"),
    )
)

with_activity = (
    grouped
    .crossJoin(grouped.agg(F.max("n_country_is_not_null").alias("overall_max_n_country_is_not_null")))
    .filter(F.col("n_country_is_not_null") < F.col("overall_max_n_country_is_not_null"))
    .drop("overall_max_n_country_is_not_null")
)

sequenced = (
    with_activity
    .select(
        "signup_date_ym",
        "row_count",
        "n_country_is_not_null",
        "n_country_is_not_null_2",
        F.round(F.cume_dist().over(w_row_count_n_country_is_not_n), 3).alias("total_birth_year_cume_dist"),
        F.max("n_country_is_not_null").over(w_row_count_n_country_is_not_n_2).alias("max_n_country_is_not_null_so_far"),
        F.lead("row_count", 1, 0).over(w_row_count_n_country_is_not_n_3).alias("next_row_count"),
        F.dense_rank().over(w_row_count_n_country_is_not_n).alias("total_birth_year_dense_rank"),
        F.ntile(4).over(w_row_count_n_country_is_not_n_3).alias("total_birth_year_quartile"),
        F.max("n_country_is_not_null_2").over(w_row_count_n_country_is_not_n_4).alias("n_country_is_not_null_2_max_2"),
    )
)

first_rows = sequenced.filter(F.col("total_birth_year_dense_rank") <= 5)

result = (
    first_rows
    .select(
        "signup_date_ym",
        "row_count",
        "n_country_is_not_null",
        "n_country_is_not_null_2",
        "max_n_country_is_not_null_so_far",
        "total_birth_year_dense_rank",
        "total_birth_year_quartile",
        "n_country_is_not_null_2_max_2",
        F.concat(F.col("signup_date_ym"), F.lit("#"), F.col("next_row_count").cast("string")).alias("signup_date_ym_ref"),
    )
    .orderBy(F.col("max_n_country_is_not_null_so_far").desc())
)
