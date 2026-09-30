from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("opened_date"), "status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")
w5 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id", "status").orderBy(F.col("opened_date").desc(), "id")
w7 = Window.orderBy("balance_whole", "id")
w8 = Window.partitionBy(F.date_trunc("YEAR", "opened_date")).orderBy(F.col("customer_id_max_balance").desc_nulls_last())
w9 = Window.partitionBy(F.month("opened_date")).orderBy("dedup_rank", "id")
w_hist = Window.partitionBy(F.month("opened_date"), "account_type").orderBy("customer_id_max_balance", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_accounts
    .filter(F.col("status").isNotNull() & F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.ceil("balance").alias("balance_whole"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_clean"),
    )
)

with_windows = (
    valid
    .filter(F.col("balance") <= 69000)
    .withColumn("last_known_balance_whole", F.last("balance_whole", ignorenulls=True).over(w))
    .withColumn("first_known_status", F.first("status", ignorenulls=True).over(w2))
    .withColumn("customer_id_max_balance", F.max("balance").over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("first_known_balance", F.first("balance", ignorenulls=True).over(w5))
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") >= F.col("s.opened_date")), "left_anti")
)

metrics = (
    kept
    .select(
        "*",
        F.ntile(5).over(w7).alias("balance_whole_bucket"),
        F.cume_dist().over(w8).alias("customer_id_max_balance_cume_dist"),
        F.row_number().over(w9).alias("row_seq"),
        F.avg("last_known_balance_whole").over(w_hist).alias("last_known_balance_whole_avg_2"),
    )
)

with_dims = (
    metrics
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.status",
        "src.status_clean",
        "src.last_known_balance_whole",
        "src.event_seq",
        "src.first_known_balance",
        F.col("c.city"),
        F.col("c.signup_date"),
    )
)

eligible = (
    with_dims
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.account_id").isin([10, 12])), "left_semi")
)

result = (
    eligible
    .alias("a")
    .join(df_customers.alias("dim"), F.col("a.customer_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.city").alias("customer_city"),
        F.col("a.city").alias("account_city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("a.event_seq")).alias("avg_event_seq"),
    )
)
