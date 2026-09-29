from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("end_date"), "customer_id").orderBy("end_date", "id").rowsBetween(-3, -1)
w2 = Window.orderBy(F.col("credit_limit").asc_nulls_first())
w3 = Window.partitionBy("status").orderBy("monthly_fee", "id")
w4 = Window.partitionBy(F.year("start_date"), "customer_id")
w5 = Window.orderBy(F.col("monthly_fee").desc(), "id")
w_grp = Window.orderBy(F.col("seq_no").desc_nulls_last())
w7 = Window.partitionBy("status")

enriched = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.name").isNotNull())
    .select("a.*", F.col("b.email"), F.col("b.credit_limit"))
)

with_windows = (
    enriched
    .select(
        "id",
        "start_date",
        "status",
        "email",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_3_prior"),
        F.cume_dist().over(w2).alias("credit_limit_cume_dist"),
        F.row_number().over(w3).alias("seq_no"),
        F.max("monthly_fee").over(w4).alias("start_date_year_max_monthly_fee"),
        F.ntile(10).over(w5).alias("monthly_fee_bucket"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("start_date") > F.to_date(F.lit("2023-05-04")))
    .select("*", F.concat_ws("/", "status", "email").alias("status_email_key"))
)

remaining_keys = (
    cleaned
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date") < "2023-04-01")
        .select("status")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "start_date",
        "email",
        "monthly_fee_avg_3_prior",
        F.rank().over(w_grp).alias("seq_no_rank"),
        F.sum("credit_limit_cume_dist").over(w7).alias("status_sum_credit_limit_cume_dist"),
        F.round((F.col("start_date_year_max_monthly_fee") / F.sum("start_date_year_max_monthly_fee").over(w7)), 4).alias("start_date_year_max_monthly_fee_share"),
    )
)

result = (
    scored
    .select(
        "start_date",
        "email",
        "seq_no_rank",
        "status_sum_credit_limit_cume_dist",
        "start_date_year_max_monthly_fee_share",
    )
    .orderBy(
        F.col("start_date").asc_nulls_first(),
        "start_date_year_max_monthly_fee_share",
    )
)
