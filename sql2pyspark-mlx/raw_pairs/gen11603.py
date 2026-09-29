from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").desc_nulls_last())

remaining_keys = (
    df_accounts
    .select("account_type", "status")
    .intersect(
        df_accounts
        .filter(F.col("opened_date") >= "2024-06-01")
        .select("account_type", "status")
    )
)

reconciled = (
    df_accounts
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        F.col("d.credit_limit"),
        F.col("d.signup_date"),
        F.col("d.age"),
    )
)

valid = (
    enriched
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "signup_date",
        "age",
        F.round((F.col("age") * 1.15), 2).alias("adjusted_age"),
        (F.col("age") * F.col("credit_limit")).alias("age_times_credit_limit"),
    )
)

scored = valid.select("*", F.dense_rank().over(w).alias("age_dense_rank"))

result = (
    scored
    .groupBy(F.date_trunc("QUARTER", "signup_date").alias("quarter_start"))
    .agg(
        F.percentile_approx("age_times_credit_limit", [0.25, 0.5, 0.75]).alias("age_times_credit_limit_quartiles"),
        F.percentile_approx("age_times_credit_limit", 0.9).alias("age_times_credit_limit_p90"),
        F.avg("age_times_credit_limit").alias("avg_age_times_credit_limit"),
    )
    .filter(F.col("age_times_credit_limit_p90") > 0)
)
