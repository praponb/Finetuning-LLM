from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("customer_id").orderBy("days_since_opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("status").orderBy("days_since_opened_date", "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("days_since_opened_date").desc(), "id")
w5 = Window.partitionBy("status", "customer_id").orderBy("days_since_opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id")
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

filtered = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "status",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "customer_id",
        "days_since_opened_date",
        F.sum("days_since_opened_date").over(w).alias("status_sum_days_since_opened_date"),
        F.sum("days_since_opened_date").over(w2).alias("cumulative_days_since_opened_date"),
        F.ntile(5).over(w_hist).alias("days_since_opened_date_bucket"),
        F.row_number().over(w4).alias("row_seq"),
        F.sum("days_since_opened_date").over(w5).alias("cumulative_days_since_opened_date_2"),
    )
)

kept_keys = (
    enriched
    .select("id", "customer_id")
    .exceptAll(
        df_accounts
        .filter(F.col("account_type") == "SAVINGS")
        .select("id", "customer_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_type") == "CREDIT"), "left_semi")
)

not_flagged = (
    kept
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_type") == "REFUND"), "left_anti")
)

metrics = (
    not_flagged
    .select(
        "*",
        F.stddev_samp("row_seq").over(w6).alias("customer_id_stddev_samp_row_seq"),
    )
)

result = (
    metrics
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "days_since_opened_date",
        "status_sum_days_since_opened_date",
        "row_seq",
        "cumulative_days_since_opened_date_2",
        "customer_id_stddev_samp_row_seq",
        F.col("max_order_amount"),
    )
    .orderBy("status_sum_days_since_opened_date")
)
