from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_type").orderBy("balance", "id")
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("account_type").orderBy("running_count_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.substring("status", 1, 2).alias("status_prefix"),
        F.date_sub("opened_date", 44).alias("opened_date_minus_44d"),
    )
)

scored = (
    valid
    .filter(F.col("balance") < 77000)
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "opened_date_minus_44d",
        F.avg("balance").over(w).alias("running_avg_balance"),
        F.max("balance").over(w).alias("running_max_balance"),
        F.count("balance").over(w).alias("running_count_balance"),
        F.lead("balance", 2).over(w2).alias("next_balance"),
    )
)

metrics = (
    scored
    .withColumn("running_max_balance_avg_6_prior", F.avg("running_max_balance").over(w3))
    .withColumn("cumulative_running_count_balance", F.sum("running_count_balance").over(w4))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.opened_date_minus_44d")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "account_type",
        "opened_date",
        "opened_date_minus_44d",
        "running_avg_balance",
        "running_max_balance",
        "running_count_balance",
        "next_balance",
        "running_max_balance_avg_6_prior",
        F.when(F.col("account_type").rlike("^(CHECKING|SAVINGS)$"), "GROUP_B").when(F.col("account_type").rlike("^(CREDIT|LOAN)$"), "EXTENDED").otherwise("GROUP_A").alias("account_type_zone"),
    )
    .orderBy("account_type")
)
