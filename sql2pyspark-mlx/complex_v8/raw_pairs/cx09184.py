from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_country_is_not_null_countr = Window.partitionBy("n_country_is_not_null", "country")
w_all_by_distinct_ids = Window.orderBy("distinct_ids", "country")
w_country_distinct_ids_by_sign = Window.partitionBy("country", "distinct_ids").orderBy("signup_date_span_days")
w_all_by_prev3_max_birth_year = Window.orderBy(F.col("prev3_max_birth_year").asc_nulls_last(), "country")

scoped_listeners = (
    df_listeners
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "birth_year",
        F.lower(F.trim(F.col("country"))).alias("country_clean"),
        F.date_format(F.col("signup_date"), "MMM yyyy").alias("signup_date_label"),
    )
)

without_matches = (
    scoped_listeners.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (~F.col("p.device").isin("phone", "tv")), "left_anti")
)

stats = (
    without_matches
    .groupBy("country")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.datediff(F.max("signup_date"), F.min("signup_date")).alias("signup_date_span_days"),
        F.sum(F.when(F.col("country").isNotNull(), 1).otherwise(0)).alias("n_country_is_not_null"),
        F.max("birth_year").alias("max_birth_year"),
    )
)

metrics = (
    stats
    .select(
        "*",
        F.sum("n_country_is_not_null").over(w_n_country_is_not_null_countr).alias("n_country_is_not_null_country_sum_n_country_is_n"),
        F.round(F.col("max_birth_year") / F.nullif(F.sum("max_birth_year").over(w_n_country_is_not_null_countr), F.lit(0)), 4).alias("max_birth_year_share_of_n_country_is_not_null_co"),
        F.lag("max_birth_year", 3).over(w_all_by_distinct_ids).alias("prev3_max_birth_year"),
        F.dense_rank().over(w_country_distinct_ids_by_sign).alias("signup_date_span_days_dense_rank"),
    )
)

annotated = (
    metrics
    .filter(F.col("country").isin("JP", "BR"))
    .select(
        "*",
        F.when(F.col("country").isin("KR", "UK"), "group_a").when(F.col("country") == "JP", "group_b").otherwise("other").alias("country_group"),
        F.substring(F.trim(F.col("country")), 1, 4).alias("country_prefix"),
    )
)

with_windows = (
    annotated
    .select(
        "*",
        F.lag("signup_date_span_days", 3).over(w_all_by_prev3_max_birth_year).alias("prev3_signup_date_span_days"),
        F.lead("distinct_ids").over(w_all_by_prev3_max_birth_year).alias("next_distinct_ids"),
        F.round(F.percent_rank().over(w_all_by_prev3_max_birth_year), 3).alias("prev3_max_birth_year_percent_rank"),
    )
)

result = (
    with_windows
    .groupBy("country_group")
    .pivot("country", ["JP", "TH", "KR"])
    .agg(F.min("prev3_signup_date_span_days"))
    .orderBy("country_group")
)
