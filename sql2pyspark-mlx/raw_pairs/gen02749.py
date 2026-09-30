from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter(F.col("opened_date").between("2024-05-01", "2024-11-01"))
    .withColumn("account_type_status_key", F.concat_ws(",", "account_type", "status"))
)

eligible = (
    filtered
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_anti")
)

with_dims = (
    eligible
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.segment"), F.col("b.email"))
)

joined = (
    with_dims
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.credit_limit") >= 13800)
    .select("e.*", F.col("c.name"), F.col("c.city"), F.col("c.age"))
)

result = (
    joined
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["country", "status"], ["country"], []],
        F.col("d.country").alias("country"),
        F.col("e.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.balance")).alias("max_balance"),
        F.grouping_id().alias("level_id"),
    )
)
