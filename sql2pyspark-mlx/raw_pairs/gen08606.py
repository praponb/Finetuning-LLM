from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("segment").orderBy("credit_limit", "id")
w_recent = Window.partitionBy("account_type").orderBy("signup_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("opened_date", "id")
w5 = Window.partitionBy("city", "segment").orderBy(F.col("credit_limit").asc_nulls_first())
w_ordered = Window.partitionBy("account_type")
w7 = Window.partitionBy("segment").orderBy(F.col("credit_limit").asc_nulls_first(), "id")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w9 = Window.partitionBy("segment").orderBy("credit_limit_bucket", "id")
w_grp = Window.partitionBy("account_type").orderBy("credit_limit_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("segment")
w12 = Window.partitionBy("status").orderBy("prev_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-02-06").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

valid_customers = (
    valid
    .filter(F.col("city").isNotNull() & F.col("segment").isin(["Corporate", "Consumer", "SMB"]))
    .withColumn("signup_date_period", F.date_trunc("week", "signup_date"))
    .withColumn("segment_group", F.when(F.col("segment").isin(["SMB", "Corporate"]), "Priority").otherwise("Other"))
)

with_last = (
    valid_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.city",
        "s.segment",
        "s.credit_limit",
        "s.signup_date_period",
        F.col("c.account_type"),
        F.col("c.opened_date"),
    )
)

metrics = (
    with_last
    .select(
        "*",
        F.ntile(10).over(w2).alias("credit_limit_bucket"),
        F.first("city", ignorenulls=True).over(w_recent).alias("first_known_city"),
        F.lag("credit_limit", 1).over(w4).alias("prev_credit_limit"),
        F.percent_rank().over(w5).alias("credit_limit_percent_rank"),
        F.stddev_samp("credit_limit").over(w_ordered).alias("account_type_stddev_samp_credit_limit"),
    )
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest_match = (
    deduped
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.status"),
        F.col("c.account_type").alias("account_account_type"),
    )
)

result = (
    latest_match
    .filter(F.col("account_type") == "SAVINGS")
    .select(
        "id",
        "segment",
        "signup_date_period",
        "credit_limit_bucket",
        "prev_credit_limit",
        "account_type_stddev_samp_credit_limit",
        "status",
        F.row_number().over(w9).alias("seq_no"),
        F.sum("account_type_stddev_samp_credit_limit").over(w_grp).alias("cumulative_account_type_stddev_samp_credit_limit"),
        F.avg("credit_limit_percent_rank").over(w11).alias("segment_avg_credit_limit_percent_rank"),
        F.last("account_account_type", ignorenulls=True).over(w12).alias("last_known_account_account_type"),
    )
)
