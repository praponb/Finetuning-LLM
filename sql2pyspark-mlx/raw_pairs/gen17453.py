from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("signup_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id")
w3 = Window.orderBy("opened_date", "id").rowsBetween(-3, -1)

base = (
    df_accounts
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-03-23")))
    .select(
        "*",
        F.initcap(F.trim("account_type")).alias("account_type_title"),
        F.substring("account_type", 1, 4).alias("account_type_prefix"),
    )
)

enriched = (
    base
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select("src.*", F.col("d.age"), F.col("d.credit_limit"), F.col("d.signup_date"))
)

base_accounts = (
    enriched
    .filter(F.col("customer_id").isNotNull())
    .withColumn("status_upper", F.upper("status"))
)

latest = (
    base_accounts
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    latest
    .filter(F.col("balance") < 45000)
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "account_type_title",
        "age",
        "signup_date",
        F.sum("age").over(w2).alias("customer_id_sum_age"),
        F.avg("credit_limit").over(w3).alias("credit_limit_avg_3_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "age",
        "signup_date",
        "customer_id_sum_age",
        "credit_limit_avg_3_prior",
    )
    .orderBy(F.col("status").asc_nulls_last())
)
