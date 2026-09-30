from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("segment", "city").orderBy("age", "id")
w3 = Window.orderBy("credit_limit", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("segment").orderBy("signup_date", "id")
w6 = Window.orderBy("signup_date", "id")
w7 = Window.partitionBy("segment").orderBy("prev_age_avg_2", "id")
w8 = Window.partitionBy("segment").orderBy(F.col("signup_date").desc_nulls_last(), "id")
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

scored = (
    df_customers
    .select(
        "id",
        "segment",
        "signup_date",
        "is_active",
        F.ntile(3).over(w).alias("credit_limit_bucket"),
        F.lead("age", 1).over(w2).alias("next_age"),
        F.avg("age").over(w3).alias("age_avg_2"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "segment",
        "signup_date",
        F.sum("credit_limit_bucket").over(w4).alias("cumulative_credit_limit_bucket"),
        F.lag("age_avg_2", 1).over(w5).alias("prev_age_avg_2"),
        F.lead("credit_limit_bucket", 1, 0).over(w6).alias("next_credit_limit_bucket"),
    )
)

with_windows_customers = (
    with_windows
    .filter(F.col("prev_age_avg_2") <= 58)
    .withColumn("event_seq", F.row_number().over(w7))
    .withColumn("prev_cumulative_credit_limit_bucket", F.lag("cumulative_credit_limit_bucket", 1).over(w7))
)

deduped = (
    with_windows_customers
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest_match = (
    deduped
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.segment",
        "s.cumulative_credit_limit_bucket",
        "s.next_credit_limit_bucket",
        "s.prev_cumulative_credit_limit_bucket",
        F.col("c.account_type").alias("account_account_type"),
        F.col("c.status").alias("account_status"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "cumulative_credit_limit_bucket",
        "next_credit_limit_bucket",
        "prev_cumulative_credit_limit_bucket",
        "account_account_type",
    )
    .orderBy(F.col("cumulative_credit_limit_bucket").desc_nulls_last())
)
