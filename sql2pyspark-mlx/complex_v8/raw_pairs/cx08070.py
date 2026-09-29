from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_signup_date_year = Window.partitionBy(F.year("signup_date"))
w_all_by_birth_year_bucket_tie = Window.orderBy(F.col("birth_year_bucket").desc())
w_all_by_birth_year = Window.orderBy(F.col("birth_year").desc(), "id")

scoped_listeners = (
    df_listeners
    .filter(F.array_contains(F.col("interests"), "kpop"))
    .select(
        "id",
        "username",
        "signup_date",
        "birth_year",
        "interests",
        (F.floor(F.col("birth_year") / 10) * 10).alias("birth_year_bucket"),
    )
)

metrics = (
    scoped_listeners
    .select(
        "*",
        F.round(F.col("birth_year") - F.avg("birth_year").over(w_signup_date_year), 2).alias("birth_year_vs_signup_date_year_avg"),
        F.dense_rank().over(w_all_by_birth_year_bucket_tie).alias("birth_year_bucket_dense_rank"),
        F.round(F.col("birth_year") / F.nullif(F.sum("birth_year").over(w_signup_date_year), F.lit(0)), 4).alias("birth_year_share_of_signup_date_year"),
        F.row_number().over(w_all_by_birth_year).alias("rn"),
    )
)

result = (
    metrics
    .filter(F.array_contains(F.col("interests"), "indie"))
    .select(
        "id",
        "username",
        "signup_date",
        "interests",
        "birth_year_bucket",
        "birth_year_bucket_dense_rank",
        "rn",
    )
)
