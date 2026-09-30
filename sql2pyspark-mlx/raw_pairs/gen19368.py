from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.orderBy("credit_limit", "id")
w3 = Window.orderBy("customer_credit_limit", "id")
w4 = Window.orderBy("customer_credit_limit", "id").rowsBetween(-5, Window.currentRow)

with_dims = (
    df_accounts
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .filter(F.col("c.is_active"))
    .select(
        "e.id",
        "e.customer_id",
        "e.balance",
        "e.opened_date",
        F.col("c.credit_limit"),
        F.col("c.signup_date"),
    )
)

joined = (
    with_dims
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        "a.*",
        F.col("c.segment"),
        F.col("c.credit_limit").alias("customer_credit_limit"),
    )
)

with_latest = (
    joined
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.balance",
        "s.credit_limit",
        "s.customer_credit_limit",
        F.col("c.txn_time"),
    )
)

scored = (
    with_latest
    .select(
        "*",
        F.lag("customer_credit_limit", 2, 0).over(w2).alias("prev_customer_credit_limit"),
        F.row_number().over(w3).alias("seq_no"),
        F.avg("credit_limit").over(w4).alias("credit_limit_avg_5"),
    )
)

cleaned = (
    scored
    .filter(F.col("customer_credit_limit") > 13200)
    .select(
        "id",
        "balance",
        "credit_limit",
        "txn_time",
        F.date_format("txn_time", "yyyy-MM-dd").alias("txn_time_label"),
    )
)

summary = (
    cleaned
    .groupBy(
        F.date_trunc("week", "txn_time").alias("week_start"),
        F.when(F.col("balance") >= 78000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.stddev_samp("balance").alias("balance_stddev"),
    )
)

result = (
    summary
    .select("week_start", "balance_tier", "max_credit_limit", "balance_stddev")
    .orderBy("week_start", F.col("balance_stddev").desc())
)
