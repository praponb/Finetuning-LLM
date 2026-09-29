from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("months_since_opened_date", "id")
w2 = Window.partitionBy("status").orderBy(F.col("months_since_opened_date").desc())
w3 = Window.partitionBy(F.month("opened_date"), "country")
w4 = Window.partitionBy("country").orderBy(F.col("next_months_since_opened_date").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("transaction_txn_type", "country").orderBy("months_since_opened_date_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("event_seq").desc())

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-04-13").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        (F.datediff(F.current_date(), "opened_date") / 30).alias("months_since_opened_date"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("resolved_status"),
    )
)

enriched = (
    cleaned
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.country"))
)

with_windows = (
    enriched
    .filter(F.col("customer_id") == 7)
    .select(
        "id",
        "balance",
        "opened_date",
        "months_since_opened_date",
        "country",
        F.lead("months_since_opened_date", 2).over(w).alias("next_months_since_opened_date"),
        F.cume_dist().over(w2).alias("months_since_opened_date_cume_dist"),
    )
)

enriched_accounts = (
    with_windows
    .select(
        "id",
        "opened_date",
        "country",
        "months_since_opened_date_cume_dist",
        F.row_number().over(w).alias("event_seq"),
        F.max("months_since_opened_date_cume_dist").over(w3).alias("opened_date_month_max_months_since_opened_date_cume_dist"),
        F.percent_rank().over(w4).alias("next_months_since_opened_date_percent_rank"),
    )
)

cleaned_accounts = (
    enriched_accounts
    .filter(F.col("opened_date") > F.lit("2023-08-15").cast("date"))
    .select(
        "*",
        (F.col("event_seq") * F.col("months_since_opened_date_cume_dist")).alias("event_seq_times_months_since_opened_date_cume_dist"),
        F.coalesce("country", F.lit("NONE")).alias("resolved_country"),
    )
)

with_last = (
    cleaned_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.months_since_opened_date_cume_dist",
        "s.event_seq",
        "s.next_months_since_opened_date_percent_rank",
        "s.resolved_country",
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

with_windows_accounts = (
    with_last
    .select(
        "id",
        "months_since_opened_date_cume_dist",
        "event_seq",
        "resolved_country",
        "transaction_txn_type",
        F.min("event_seq").over(w6).alias("running_min_event_seq"),
        F.count("event_seq").over(w6).alias("running_count_event_seq"),
        F.sum("event_seq").over(w6).alias("running_sum_event_seq"),
        F.max("event_seq").over(w6).alias("running_max_event_seq"),
        F.percent_rank().over(w7).alias("event_seq_percent_rank"),
    )
)

result = (
    with_windows_accounts
    .select(
        "id",
        "months_since_opened_date_cume_dist",
        "event_seq",
        "resolved_country",
        "transaction_txn_type",
        "running_max_event_seq",
        F.when(F.col("transaction_txn_type").rlike("^(CREDIT|REFUND)$"), "EXTENDED").when(F.col("transaction_txn_type").rlike("^(DEBIT|FEE)$"), "GROUP_A").otherwise("CORE").alias("transaction_txn_type_zone"),
    )
    .orderBy(F.col("transaction_txn_type_zone").desc())
)
