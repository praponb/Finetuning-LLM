from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("signup_date_month").desc_nulls_first())
w2 = Window.orderBy(F.col("age").desc())
w3 = Window.orderBy("signup_date_month_dense_rank")
w4 = Window.orderBy(F.col("age_rank").desc())
w5 = Window.orderBy(F.col("signup_date_month_dense_rank_dense_rank").desc_nulls_last())

cleaned = (
    df_customers
    .filter(F.col("segment") == "SMB")
    .select(
        "id",
        "name",
        "signup_date",
        "age",
        F.month("signup_date").alias("signup_date_month"),
    )
)

remaining_keys = (
    cleaned
    .select("id")
    .intersectAll(
        df_customers
        .filter(F.col("country").isin(["UK", "US", "JP"]))
        .select("id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.device").isin(["desktop", "mobile"])), "left_anti")
)

enriched = (
    kept
    .select(
        "name",
        "signup_date",
        F.dense_rank().over(w).alias("signup_date_month_dense_rank"),
        F.rank().over(w2).alias("age_rank"),
    )
)

filtered = (
    enriched
    .filter(F.length("name") > 8)
    .withColumn("age_rank_minus_signup_date_month_dense_rank", (F.col("age_rank") - F.col("signup_date_month_dense_rank")))
)

with_windows = (
    filtered
    .select(
        "name",
        "signup_date",
        "signup_date_month_dense_rank",
        "age_rank_minus_signup_date_month_dense_rank",
        F.dense_rank().over(w3).alias("signup_date_month_dense_rank_dense_rank"),
        F.cume_dist().over(w4).alias("age_rank_cume_dist"),
    )
)

scored = (
    with_windows
    .withColumn("signup_date_month_dense_rank_dense_rank_dense_rank", F.dense_rank().over(w5))
)

result = (
    scored
    .groupBy(F.date_trunc("month", "signup_date").alias("activity_month"))
    .agg(
        F.sum("signup_date_month_dense_rank_dense_rank").alias("total_signup_date_month_dense_rank_dense_rank"),
        F.round(F.avg("signup_date_month_dense_rank_dense_rank_dense_rank"), 2).alias("avg_signup_date_month_dense_rank_dense_rank_dense_rank"),
    )
    .orderBy(F.col("activity_month").desc_nulls_last())
)
