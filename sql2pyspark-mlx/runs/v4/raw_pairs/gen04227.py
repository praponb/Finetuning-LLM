from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("opened_date").asc_nulls_first(), "id")

base = (
    df_accounts
    .filter(F.col("balance") < 33000)
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.lower("account_type").alias("account_type_lower"),
    )
)

deduped = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < F.col("s.opened_date")), "left_semi")
)

enriched = (
    not_flagged
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.signup_date"))
)

result = (
    enriched
    .groupBy(
        F.date_trunc("YEAR", "signup_date").alias("year_start"),
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
    )
    .agg(
        F.count_if(F.col("status") == "CLOSED").alias("closed_events"),
        F.count_if(F.col("status") == "FROZEN").alias("frozen_events"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count_if(F.col("status") == "OPEN").alias("open_events"),
    )
)
