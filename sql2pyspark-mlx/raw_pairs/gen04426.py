from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id", "txn_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id", "status").orderBy(F.col("balance").desc_nulls_first())
w4 = Window.partitionBy("txn_type")
w5 = Window.partitionBy(F.year("opened_date"), "txn_type").orderBy(F.col("opened_date").desc(), "id")
w6 = Window.partitionBy("customer_id").orderBy("balance", "id")

valid = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-10-07").cast("date"))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.coalesce("status", F.lit("Unknown")).alias("status_filled"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type"), F.col("c.amount"))
)

metrics = (
    latest_match
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        "status_filled",
        "txn_type",
        F.sum("balance").over(w2).alias("running_balance"),
        F.dense_rank().over(w3).alias("balance_dense_rank"),
        F.avg("amount").over(w4).alias("txn_type_avg_amount"),
        F.row_number().over(w5).alias("row_seq"),
        F.ntile(10).over(w6).alias("balance_bucket"),
    )
)

latest_match_accounts = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.status",
        "s.status_filled",
        "s.txn_type",
        "s.running_balance",
        "s.balance_bucket",
        F.col("c.txn_time"),
    )
)

with_dims = (
    latest_match_accounts
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.is_active"))
    .select("a.*", F.col("d.segment"))
)

result = (
    with_dims
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["city", "country"], ["city"]],
        F.col("d.city").alias("city"),
        F.col("d.country").alias("country"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("a.running_balance")).alias("total_running_balance"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy("rows_total")
)
