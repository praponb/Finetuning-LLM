from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("balance").asc_nulls_first(), "id")
w2 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(-6, -1)

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select("*", F.coalesce("balance", F.lit(0)).alias("balance_filled"))
)

latest = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

base = (
    latest
    .filter(F.col("status").isNotNull() & (F.col("status").eqNullSafe(F.lit("OPEN"))))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "balance_filled",
        F.concat("account_type", F.lit(" - "), "status").alias("account_type_status"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_anti")
)

scored = (
    not_flagged
    .filter(F.col("status") == "OPEN")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.sum("balance").over(w2).alias("cumulative_balance"),
        F.sum("balance").over(w3).alias("cumulative_balance_2"),
        F.avg("balance").over(w4).alias("balance_avg_6_prior"),
    )
)

result = (
    scored
    .select("id", "customer_id", "balance", "opened_date", "cumulative_balance_2")
    .orderBy("id")
    .limit(20)
)
