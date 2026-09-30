from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status").orderBy("balance", "id")
w_part = Window.orderBy("prev_is_credit", "id")
w3 = Window.partitionBy("customer_id").orderBy("prev_balance", "id")

valid = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-06-04"))) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.when(F.col("account_type") == "CREDIT", 1).otherwise(0).alias("is_credit"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "is_credit",
        F.lag("balance").over(w_ordered).alias("prev_balance"),
        F.lead("balance").over(w_ordered).alias("next_balance"),
        F.lag("is_credit").over(w_ordered).alias("prev_is_credit"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_anti")
)

with_dims = (
    not_flagged
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.is_credit",
        "s.prev_balance",
        "s.next_balance",
        "s.prev_is_credit",
        "s.seq_no",
        F.col("p.city"),
    )
)

eligible = (
    with_dims
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_type").isin(["CREDIT", "REFUND", "DEBIT"])), "left_anti")
)

result = (
    eligible
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "prev_balance",
        "seq_no",
        F.ntile(3).over(w_part).alias("prev_is_credit_bucket"),
        F.row_number().over(w3).alias("row_seq"),
    )
)
