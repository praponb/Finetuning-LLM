from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.year("signup_date"), "customer_id").orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)
w_hist = Window.partitionBy("customer_id").orderBy("transaction_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.year("signup_date")).orderBy("transaction_amount", "id").rowsBetween(-5, Window.currentRow)

with_dims = (
    df_accounts
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.opened_date",
        "a.status",
        F.col("p.signup_date"),
    )
)

latest_match = (
    with_dims
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type"), F.col("c.amount").alias("transaction_amount"))
)

with_windows = (
    latest_match
    .filter(F.col("transaction_amount") <= 3400)
    .select(
        "id",
        "customer_id",
        "opened_date",
        "transaction_amount",
        F.avg("transaction_amount").over(w2).alias("transaction_amount_avg_3"),
        F.sum("transaction_amount").over(w_hist).alias("cumulative_transaction_amount"),
        F.sum("transaction_amount").over(w_hist).alias("running_transaction_amount"),
        F.avg("transaction_amount").over(w4).alias("transaction_amount_avg_5"),
    )
)

matched_keys = (
    with_windows
    .select("id")
    .intersect(
        df_accounts
        .filter(F.col("customer_id").isin([5, 36]))
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims_accounts = (
    verified
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "e.customer_id",
        "e.opened_date",
        "e.cumulative_transaction_amount",
        "e.running_transaction_amount",
        "e.transaction_amount_avg_5",
        F.col("d.segment"),
    )
)

result = (
    with_dims_accounts
    .select(
        "customer_id",
        "opened_date",
        "cumulative_transaction_amount",
        "transaction_amount_avg_5",
        "segment",
        F.when(F.col("segment").rlike("^(Consumer|SMB)$"), "GROUP_B").when(F.col("segment").rlike("^(Corporate)$"), "GROUP_A").otherwise("PRIORITY").alias("segment_zone"),
    )
    .orderBy(F.col("cumulative_transaction_amount").desc_nulls_last(), "segment")
)
