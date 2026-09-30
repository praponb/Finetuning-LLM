from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("MONTH", "signup_date"), "segment").orderBy(F.col("age").desc_nulls_last())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-05-07").cast("date")) & F.col("segment").isNotNull())
    .select("*", F.when(F.col("country") == "SG", 1).otherwise(0).alias("is_sg"))
)

scored = (
    valid
    .select(
        "id",
        "name",
        "country",
        "segment",
        "signup_date",
        "credit_limit",
        F.min("credit_limit").over(w).alias("last3_min_credit_limit"),
        F.sum("credit_limit").over(w).alias("last3_sum_credit_limit"),
        F.count("credit_limit").over(w).alias("last3_count_credit_limit"),
        F.cume_dist().over(w2).alias("age_cume_dist"),
    )
)

cleaned = (
    scored
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "country",
        "segment",
        "signup_date",
        "credit_limit",
        "last3_min_credit_limit",
        "last3_sum_credit_limit",
        "last3_count_credit_limit",
        F.coalesce("segment", F.lit("unassigned")).alias("segment_filled"),
        F.coalesce("segment", F.lit("NONE")).alias("resolved_segment"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.signup_date",
        "s.credit_limit",
        "s.last3_sum_credit_limit",
        "s.segment_filled",
        F.col("c.status").alias("account_status"),
        F.col("c.balance"),
    )
)

kept = (
    with_latest
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "left_anti")
)

result = (
    kept
    .rollup("account_status")
    .agg(
        F.size(F.collect_set("account_status")).alias("num_account_status"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("num_account_status") > 1)
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
