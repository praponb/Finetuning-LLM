from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy(F.month("opened_date"), "status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("account_type").orderBy("balance", "id")
w3 = Window.partitionBy("city").orderBy(F.col("balance").asc_nulls_last())
w4 = Window.partitionBy("status", "account_type").orderBy("next_balance", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("first_known_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy(F.col("first_known_balance").desc(), "id")
w7 = Window.partitionBy("status", "account_type").orderBy("balance", "id")

with_dims = (
    df_accounts
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.city"))
)

with_windows = (
    with_dims
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.first("balance", ignorenulls=True).over(w_ordered).alias("first_known_balance"),
        F.lead("balance", 2).over(w_recent).alias("next_balance"),
        F.dense_rank().over(w3).alias("balance_dense_rank"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.amount") >= 1700), "left_anti")
)

scored = (
    not_flagged
    .withColumn("balance_dense_rank_avg_3", F.avg("balance_dense_rank").over(w4))
    .withColumn("first_known_status", F.first("status", ignorenulls=True).over(w5))
    .withColumn("first_known_balance_bucket", F.ntile(5).over(w6))
)

metrics = (
    scored
    .filter(F.col("first_known_balance") <= 22000)
    .select(
        "id",
        "balance",
        "status",
        "first_known_balance",
        "next_balance",
        "balance_dense_rank",
        F.lag("balance_dense_rank").over(w7).alias("prev_balance_dense_rank"),
        F.lead("balance_dense_rank").over(w7).alias("next_balance_dense_rank"),
        F.lag("next_balance").over(w7).alias("prev_next_balance"),
        F.lead("next_balance").over(w7).alias("next_next_balance"),
        F.row_number().over(w7).alias("seq_no"),
    )
)

result = (
    metrics
    .select(
        "id",
        "balance",
        "first_known_balance",
        "next_balance",
        "prev_balance_dense_rank",
        "next_balance_dense_rank",
        "next_next_balance",
        "seq_no",
    )
)
