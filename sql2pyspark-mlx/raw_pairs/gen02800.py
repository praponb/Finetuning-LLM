from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "city").orderBy("balance", "id")
w2 = Window.partitionBy("status").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)
w_running = Window.orderBy(F.col("customer_age").asc_nulls_first())
w5 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("customer_id", "customer_city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_accounts
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.account_type",
        "src.balance",
        "src.opened_date",
        "src.status",
        F.col("p.city"),
        F.col("p.age"),
        F.col("p.signup_date"),
    )
)

joined = (
    enriched
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.country") != "JP")
    .select(
        "src.*",
        F.col("b.name"),
        F.col("b.age").alias("customer_age"),
        F.col("b.credit_limit"),
    )
)

cleaned = (
    joined
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-06-02"))) & F.col("customer_id").isin([12, 21, 30]))
    .withColumn("city_account_type", F.concat("city", F.lit(" - "), "account_type"))
    .withColumn("resolved_status", F.coalesce("status", F.lit("UNKNOWN")))
)

enriched_accounts = (
    cleaned
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "city",
        "signup_date",
        "name",
        "customer_age",
        "resolved_status",
        F.ntile(3).over(w).alias("balance_bucket"),
        F.last("status", ignorenulls=True).over(w2).alias("last_known_status"),
        F.avg("age").over(w3).alias("age_avg_3"),
    )
)

not_flagged = (
    enriched_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") >= F.col("s.opened_date")), "left_anti")
)

with_dims = (
    not_flagged
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select(
        "s.*",
        F.col("b.name").alias("customer_name"),
        F.col("b.city").alias("customer_city"),
        F.col("b.signup_date").alias("customer_signup_date"),
    )
)

scored = (
    with_dims
    .select(
        "id",
        "balance",
        "city",
        "customer_age",
        "balance_bucket",
        "age_avg_3",
        "customer_name",
        F.percent_rank().over(w_running).alias("customer_age_percent_rank"),
        F.sum("customer_age").over(w5).alias("running_customer_age"),
        F.first("age_avg_3", ignorenulls=True).over(w6).alias("first_known_age_avg_3"),
        F.sum("customer_age").over(w7).alias("cumulative_customer_age"),
    )
)

result = (
    scored
    .select(
        "id",
        "balance",
        "city",
        "balance_bucket",
        "age_avg_3",
        "customer_name",
        "customer_age_percent_rank",
        "running_customer_age",
        "first_known_age_avg_3",
        F.when(F.col("city").rlike("^(Tokyo|Singapore)$"), "CORE").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy(F.col("running_customer_age").asc_nulls_last())
)
