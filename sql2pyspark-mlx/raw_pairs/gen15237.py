from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("status", "customer_id").orderBy("age", "id")
w3 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

filtered = (
    df_accounts
    .filter((~F.col("status").eqNullSafe(F.lit("FROZEN"))) & F.col("balance").between(69000, 77000))
    .select("*", F.substring("account_type", 1, 3).alias("account_type_prefix"))
)

with_dims = (
    filtered
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .filter(~F.col("d.is_active"))
    .select(
        "src.id",
        "src.customer_id",
        "src.balance",
        "src.status",
        "src.account_type_prefix",
        F.col("d.age"),
        F.col("d.credit_limit"),
        F.col("d.city"),
    )
)

latest_match = (
    with_dims
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time"), F.col("c.txn_type"))
)

with_windows = (
    latest_match
    .select(
        "id",
        "customer_id",
        "balance",
        "age",
        "city",
        "txn_time",
        "txn_type",
        F.lead("balance").over(w_recent).alias("next_balance"),
        F.lag("balance").over(w_recent).alias("prev_balance"),
        F.lead("credit_limit").over(w_recent).alias("next_credit_limit"),
        F.lag("credit_limit").over(w_recent).alias("prev_credit_limit"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.sum("credit_limit").over(w3).alias("credit_limit_sum_90d"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "customer_id",
        "balance",
        "age",
        "city",
        "prev_balance",
        "next_credit_limit",
        "prev_credit_limit",
        "row_seq",
    )
    .orderBy(F.col("row_seq").desc_nulls_first())
)
