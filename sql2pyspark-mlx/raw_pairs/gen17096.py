from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-2, -1)

cleaned = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-09-12").cast("date")) & ~F.col("account_type").isin(["LOAN", "SAVINGS"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.dayofweek("opened_date").alias("opened_date_dayofweek"),
        F.coalesce("status", F.lit("NONE")).alias("status_clean"),
    )
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.age") < 58)
    .select(
        "e.id",
        "e.customer_id",
        "e.opened_date",
        "e.opened_date_dayofweek",
        "e.status_clean",
        F.col("d.signup_date"),
    )
)

scored = (
    with_dims
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status_clean",
        "signup_date",
        F.lag("opened_date_dayofweek").over(w_recent).alias("prev_opened_date_dayofweek"),
        F.lead("opened_date_dayofweek").over(w_recent).alias("next_opened_date_dayofweek"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.avg("opened_date_dayofweek").over(w2).alias("opened_date_dayofweek_avg_2_prior"),
    )
)

joined = (
    scored
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.signup_date",
        "s.prev_opened_date_dayofweek",
        "s.next_opened_date_dayofweek",
        "s.row_seq",
        "s.opened_date_dayofweek_avg_2_prior",
        F.col("c.city"),
        F.col("c.signup_date").alias("customer_signup_date"),
    )
)

base = (
    joined
    .filter(F.col("city").eqNullSafe(F.lit("Chiang Mai")))
    .select(
        "id",
        "prev_opened_date_dayofweek",
        "opened_date_dayofweek_avg_2_prior",
        "customer_signup_date",
        F.datediff("customer_signup_date", "signup_date").alias("days_signup_date_to_customer_signup_date"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_type") == "FEE"), "left_semi")
)

result = (
    kept
    .withColumn("prev_opened_date_dayofweek_tier", F.when(F.col("prev_opened_date_dayofweek") >= 78, "HIGH").otherwise("LOW"))
    .rollup("prev_opened_date_dayofweek_tier")
    .agg(
        F.round(F.avg("days_signup_date_to_customer_signup_date"), 2).alias("avg_days_signup_date_to_customer_signup_date"),
        F.avg("opened_date_dayofweek_avg_2_prior").alias("avg_opened_date_dayofweek_avg_2_prior"),
        F.percentile_approx("days_signup_date_to_customer_signup_date", 0.95).alias("days_signup_date_to_customer_signup_date_p95"),
        F.percentile_approx("prev_opened_date_dayofweek", [0.25, 0.5, 0.75]).alias("prev_opened_date_dayofweek_quartiles"),
        F.round(F.avg("prev_opened_date_dayofweek"), 2).alias("avg_prev_opened_date_dayofweek"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("days_signup_date_to_customer_signup_date_p95") > 0)
    .orderBy(F.col("avg_prev_opened_date_dayofweek").desc_nulls_last())
)
