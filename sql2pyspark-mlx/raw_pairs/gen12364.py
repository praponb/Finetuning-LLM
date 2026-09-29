from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc_nulls_last())
w_recent = Window.partitionBy(F.month("opened_date"), "customer_id").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type").orderBy("dedup_rank", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("txn_type", "customer_id")
w6 = Window.partitionBy("customer_id")
w7 = Window.partitionBy("txn_type")
w8 = Window.partitionBy("txn_type").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("customer_id", "txn_type").orderBy("amount", "id").rowsBetween(-2, -1)

ranked = (
    df_accounts
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    ranked
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "dedup_rank",
        F.last("balance", ignorenulls=True).over(w_recent).alias("last_known_balance"),
        F.avg("balance").over(w3).alias("balance_avg_3"),
    )
)

enriched = (
    scored
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.segment") == "SMB")
    .select(
        "a.id",
        "a.customer_id",
        "a.account_type",
        "a.opened_date",
        "a.balance_avg_3",
        F.col("p.email"),
        F.col("p.age"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_semi")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.customer_id", "s.age", F.col("c.txn_type"), F.col("c.amount"))
)

scored_accounts = (
    with_latest
    .select(
        "id",
        "customer_id",
        "txn_type",
        F.avg("amount").over(w5).alias("txn_type_avg_amount"),
        F.round((F.col("age") / F.sum("age").over(w6)), 4).alias("age_share"),
        F.round((F.col("age") / F.sum("age").over(w7)), 4).alias("age_share_2"),
        F.sum("age").over(w8).alias("running_age"),
        F.avg("amount").over(w9).alias("amount_avg_2_prior"),
    )
)

with_dims = (
    scored_accounts
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select("a.*", F.col("d.signup_date"))
)

result = (
    with_dims
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["segment", "country"], ["segment"], []],
        F.col("p.segment").alias("segment"),
        F.col("p.country").alias("country"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("src.age_share")).alias("total_age_share"),
    )
)
