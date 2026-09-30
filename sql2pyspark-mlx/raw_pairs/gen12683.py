from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("credit_limit").desc())
w3 = Window.partitionBy("merchant").orderBy("credit_limit", "id")
w4 = Window.orderBy("balance", "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-07-17"))) & F.month("opened_date").isin([3, 5, 6]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.initcap(F.trim("account_type")).alias("account_type_title"),
        F.coalesce("status", F.lit("OTHER")).alias("resolved_status"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant"))
)

enriched = (
    with_last
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("e.*", F.col("p.signup_date"), F.col("p.credit_limit"), F.col("p.name"))
)

filtered = (
    enriched
    .filter((F.col("merchant") == "Amazon") & (~F.col("status").eqNullSafe(F.lit("OPEN"))))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "resolved_status",
        "merchant",
        "signup_date",
        "credit_limit",
        "name",
        F.when(F.col("merchant").isin(["Grab", "Amazon"]), "Priority").otherwise("Other").alias("merchant_group"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("resolved_status_2"),
    )
)

enriched_accounts = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w2).alias("credit_limit_dense_rank"),
        F.ntile(4).over(w3).alias("credit_limit_bucket"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

result = (
    enriched_accounts
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "merchant",
        "signup_date",
        "credit_limit",
        "name",
        F.when(F.col("name").rlike("^(Emma Brown|bob smith)$"), "GROUP_B").when(F.col("name").rlike("^(Alice Wong|  Carol Diaz)$"), "EXTENDED").otherwise("CORE").alias("name_zone"),
        F.col("min_account_balance"),
    )
    .orderBy(F.col("id").desc())
)
