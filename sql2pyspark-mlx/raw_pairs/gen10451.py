from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")
w_grp = Window.orderBy("least_credit_limit_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("country", "city").orderBy(F.col("credit_limit").asc_nulls_first())
w4 = Window.partitionBy("country").orderBy(F.col("least_credit_limit_age").desc(), "id")
w5 = Window.partitionBy("city").orderBy(F.col("credit_limit_dense_rank").desc_nulls_first(), "id")
w6 = Window.partitionBy("country").orderBy("signup_date")

filtered = (
    df_customers
    .filter(F.col("is_active"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        F.least("credit_limit", "age").alias("least_credit_limit_age"),
        F.upper("email").alias("email_upper"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

with_windows = (
    filtered
    .filter(F.month("signup_date").isin([1, 8, 12]))
    .select(
        "id",
        "name",
        "country",
        "city",
        "signup_date",
        "email_upper",
        F.lead("least_credit_limit_age", 2).over(w).alias("next_least_credit_limit_age"),
        F.first("least_credit_limit_age", ignorenulls=True).over(w_grp).alias("first_known_least_credit_limit_age"),
        F.dense_rank().over(w_part).alias("credit_limit_dense_rank"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

deduped = (
    with_windows
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "name",
        "country",
        "signup_date",
        "email_upper",
        "next_least_credit_limit_age",
        "credit_limit_dense_rank",
        "row_seq",
    )
)

latest = (
    deduped
    .withColumn("dedup_rank", F.rank().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    latest
    .filter(F.length("name") < 5)
    .select(
        "id",
        "name",
        "country",
        "signup_date",
        "email_upper",
        "next_least_credit_limit_age",
        "credit_limit_dense_rank",
        "row_seq",
        F.when(F.col("credit_limit_dense_rank") >= 7, "High").when(F.col("credit_limit_dense_rank") >= 3, "Medium").otherwise("Low").alias("credit_limit_dense_rank_level"),
        F.round("dedup_rank", 0).alias("dedup_rank_rounded"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.start_date") < F.col("s.signup_date")), "left_anti")
)

base = (
    eligible
    .filter(F.col("signup_date") > F.lit("2023-08-24").cast("date"))
    .select(
        "id",
        "name",
        "country",
        "signup_date",
        "email_upper",
        "next_least_credit_limit_age",
        "credit_limit_dense_rank",
        "row_seq",
        "credit_limit_dense_rank_level",
        "dedup_rank_rounded",
        F.col("row_seq").cast("string").alias("row_seq_num"),
    )
)

report = (
    base
    .select(
        "id",
        "name",
        "country",
        "signup_date",
        "email_upper",
        "next_least_credit_limit_age",
        "credit_limit_dense_rank",
        "row_seq",
        "credit_limit_dense_rank_level",
    )
)

result = (
    report
    .select(
        "id",
        "country",
        "signup_date",
        "email_upper",
        "next_least_credit_limit_age",
        "row_seq",
        F.when(F.col("country").rlike("^(JP|SG)$"), "PRIORITY").when(F.col("country").rlike("^(TH|US)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("country_zone"),
    )
)
