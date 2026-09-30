from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").desc_nulls_first())
w2 = Window.partitionBy(F.year("signup_date")).orderBy(F.col("credit_limit").asc_nulls_first())
w3 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc())
w4 = Window.partitionBy("city")
w5 = Window.partitionBy(F.date_trunc("month", "signup_date"), "city")

filtered = (
    df_customers
    .filter(F.col("segment").isNotNull() & (~F.col("segment").eqNullSafe(F.lit("SMB"))))
    .select(
        "id",
        "name",
        "email",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.round((F.col("age") * 0.9), 2).alias("age_adjusted"),
    )
)

remaining_keys = (
    filtered
    .select("id", "city")
    .subtract(
        df_customers
        .filter(F.month("signup_date").isin([3, 4, 6]))
        .select("id", "city")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("age_percent_rank", F.percent_rank().over(w))
    .withColumn("credit_limit_dense_rank", F.dense_rank().over(w2))
    .withColumn("credit_limit_dense_rank_2", F.dense_rank().over(w3))
)

with_windows_customers = (
    with_windows
    .filter(F.col("credit_limit_dense_rank_2") > 6)
    .withColumn("credit_limit_dense_rank_share", F.round((F.col("credit_limit_dense_rank") / F.sum("credit_limit_dense_rank").over(w4)), 4))
    .withColumn("signup_date_period_stddev_samp_credit_limit", F.stddev_samp("credit_limit").over(w5))
)

base = (
    with_windows_customers
    .filter(F.col("city").isNotNull())
    .select("*", F.concat_ws("/", "city", "email").alias("city_email_key"))
)

result = (
    base
    .withColumn("week_start", F.date_trunc("WEEK", "signup_date"))
    .cube("week_start", "city")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("credit_limit_per_london"),
        F.round(F.sum("credit_limit_dense_rank_share") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("credit_limit_dense_rank_share_per_chiang_mai"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("credit_limit_dense_rank_share_per_chiang_mai") >= 0)
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
