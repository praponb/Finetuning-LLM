from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_by_signup_date = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), "id")
w_cnt = Window.partitionBy("cnt")
w_cnt_signup_date_dense_rank_b = Window.partitionBy("cnt", "signup_date_dense_rank").orderBy("total_signup_date_percent_rank", "signup_date_dense_rank")

cleaned = (
    df_listeners
    .filter(F.lower(F.col("username")).like("%an%"))
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "interests",
    )
)

sequenced = (
    cleaned
    .select(
        "id",
        "interests",
        F.dense_rank().over(w_country_by_signup_date).alias("signup_date_dense_rank"),
        F.round(F.percent_rank().over(w_country_by_signup_date), 3).alias("signup_date_percent_rank"),
    )
)

qualified = (
    sequenced
    .crossJoin(sequenced.agg(F.max("signup_date_percent_rank").alias("overall_max_signup_date_percent_rank")))
    .filter(F.col("signup_date_percent_rank") < F.col("overall_max_signup_date_percent_rank"))
    .drop("overall_max_signup_date_percent_rank")
)

without_matches = (
    qualified
    .crossJoin(qualified.agg(F.avg("signup_date_dense_rank").alias("overall_avg_signup_date_dense_rank")))
    .filter(F.col("signup_date_dense_rank") > F.col("overall_avg_signup_date_dense_rank"))
    .drop("overall_avg_signup_date_dense_rank")
)

summary = (
    without_matches
    .groupBy("signup_date_dense_rank")
    .agg(
        F.round(F.sum("signup_date_dense_rank") / F.nullif(F.sum("signup_date_percent_rank"), F.lit(0)), 3).alias("signup_date_dense_rank_to_signup_date_percent_ra"),
        F.count("*").alias("cnt"),
        F.sum("signup_date_percent_rank").alias("total_signup_date_percent_rank"),
    )
    .filter(F.col("cnt") >= 2)
)

sequenced_2 = (
    summary
    .withColumn("signup_date_dense_rank_vs_cnt_avg", F.round(F.col("signup_date_dense_rank") - F.avg("signup_date_dense_rank").over(w_cnt), 2))
    .withColumn("next_cnt", F.lead("cnt", 1, 0).over(w_cnt_signup_date_dense_rank_b))
)

derived = (
    sequenced_2
    .select(
        "*",
        F.greatest(F.col("next_cnt"), F.col("cnt")).alias("greatest_next_cnt_cnt"),
        F.coalesce(F.col("cnt"), F.lit(0)).alias("cnt_filled"),
    )
)

result = (
    derived
    .filter(F.col("greatest_next_cnt_cnt") > 5)
    .select(
        "signup_date_dense_rank",
        "cnt",
        "signup_date_dense_rank_vs_cnt_avg",
        "next_cnt",
        F.coalesce(F.col("greatest_next_cnt_cnt"), F.lit(0)).alias("greatest_next_cnt_cnt_filled"),
    )
)
