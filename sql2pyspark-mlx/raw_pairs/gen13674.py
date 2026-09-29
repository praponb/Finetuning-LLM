from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

enriched = (
    df_customers
    .filter(F.upper(F.trim("email")) == "KANYA@MAIL.CO.TH")
    .withColumn("running_sum_age", F.sum("age").over(w))
    .withColumn("running_min_age", F.min("age").over(w))
    .withColumn("running_max_age", F.max("age").over(w))
    .withColumn("running_count_age", F.count("age").over(w))
)

valid = (
    enriched
    .filter(F.col("signup_date") >= F.to_date(F.lit("2023-08-21")))
    .select(
        "id",
        "email",
        "city",
        "is_active",
        "credit_limit",
        "running_sum_age",
        "running_max_age",
        "running_count_age",
        F.upper(F.trim("email")).alias("email_normalized"),
        F.trim("name").alias("clean_name"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.running_max_age",
        "s.running_count_age",
        "s.email_normalized",
        "s.clean_name",
        F.col("c.account_type").alias("account_account_type"),
    )
)

kept = (
    latest_match
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.monthly_fee") < 200), "left_anti")
)

result = (
    kept
    .withColumn("running_max_age_tier", F.when(F.col("running_max_age") >= 58, "LARGE").otherwise("SMALL"))
    .rollup("account_account_type", "running_max_age_tier")
    .agg(
        F.sum(F.when(F.col("account_account_type") == "CREDIT", F.col("running_count_age")).otherwise(0)).alias("credit_running_count_age"),
        F.avg(F.when(F.col("account_account_type") == "LOAN", F.col("running_max_age"))).alias("loan_avg_running_max_age"),
        F.percentile_approx("running_max_age", [0.25, 0.5, 0.75]).alias("running_max_age_quartiles"),
        F.sort_array(F.collect_set("account_account_type")).alias("account_account_type_list"),
        F.sum("running_max_age").alias("total_running_max_age"),
        F.count(F.when(F.col("account_account_type") == "CREDIT", 1)).alias("credit_rows"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("loan_avg_running_max_age").desc_nulls_last())
)
