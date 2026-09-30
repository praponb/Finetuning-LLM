from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

joined = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.signup_date"), F.col("b.email"), F.col("b.city"))
)

latest = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_last = (
    latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time").alias("transaction_txn_time"))
)

remaining_keys = (
    with_last
    .select("account_type")
    .intersect(
        df_accounts
        .filter(F.month("opened_date").isin([6, 9, 10]))
        .select("account_type")
    )
)

verified = (
    with_last
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

result = (
    verified
    .cube(
        F.when(F.col("balance") >= 82000, "LARGE").otherwise("SMALL").alias("balance_tier"),
        F.date_trunc("week", "transaction_txn_time").alias("week_start"),
    )
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.avg(F.when(F.col("status") == "OPEN", F.col("balance"))).alias("open_avg_balance"),
        F.percentile_approx("balance", 0.9).alias("balance_p90"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(F.col("distinct_status").desc())
)
