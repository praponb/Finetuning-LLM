from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("balance").desc_nulls_last())
w_part = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")
w4 = Window.partitionBy("customer_id")
w5 = Window.orderBy("balance", "id")
w6 = Window.partitionBy("account_type").orderBy("seq_no", "id")
w7 = Window.partitionBy(F.month("opened_date"), "status").orderBy("balance_bucket", "id")
w8 = Window.partitionBy(F.month("opened_date"), "customer_id").orderBy("balance_rank", "id").rowsBetween(-6, -1)
w9 = Window.orderBy("seq_no", "id")
w_hist = Window.partitionBy("account_type").orderBy("balance_bucket_avg_6_prior", "id")
w11 = Window.partitionBy(F.month("opened_date")).orderBy("balance_rank", "id")
w12 = Window.partitionBy("account_type").orderBy("balance_bucket_2", "id")
w13 = Window.partitionBy("account_type")
w14 = Window.partitionBy("account_type").orderBy("seq_no_bucket_2", "id").rowsBetween(-3, Window.currentRow)
w15 = Window.partitionBy("account_type").orderBy(F.col("prev_balance").desc())
w16 = Window.partitionBy("account_type").orderBy("seq_no_bucket_2", "id").rowsBetween(-6, Window.currentRow)
w17 = Window.partitionBy("account_type").orderBy("prev_prev_balance")
w18 = Window.orderBy("prev_balance", "id")
w19 = Window.partitionBy(F.date_trunc("YEAR", "opened_date"), "account_type")

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_anti")
)

scored = (
    kept
    .select(
        "*",
        F.rank().over(w).alias("balance_rank"),
        F.row_number().over(w_part).alias("seq_no"),
        F.ntile(5).over(w3).alias("balance_bucket"),
        F.count("balance").over(w4).alias("customer_id_count_balance"),
        F.ntile(5).over(w5).alias("balance_bucket_2"),
    )
)

enriched = (
    scored
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "balance_rank",
        "balance_bucket_2",
        F.ntile(5).over(w6).alias("seq_no_bucket"),
        F.ntile(3).over(w7).alias("balance_bucket_bucket"),
        F.avg("balance_bucket").over(w8).alias("balance_bucket_avg_6_prior"),
        F.ntile(10).over(w9).alias("seq_no_bucket_2"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

with_windows = (
    enriched
    .withColumn("prev_row_seq", F.lag("row_seq").over(w_hist))
    .withColumn("next_row_seq", F.lead("row_seq").over(w_hist))
    .withColumn("prev_balance", F.lag("balance").over(w_hist))
    .withColumn("seq_no", F.row_number().over(w_hist))
)

metrics = (
    with_windows
    .select(
        "id",
        "account_type",
        "opened_date",
        "balance_rank",
        "balance_bucket_2",
        "seq_no_bucket_2",
        "prev_row_seq",
        "next_row_seq",
        "prev_balance",
        "seq_no",
        F.ntile(10).over(w11).alias("balance_rank_bucket"),
        F.lag("prev_balance", 1).over(w12).alias("prev_prev_balance"),
        F.avg("balance_bucket_bucket").over(w13).alias("account_type_avg_balance_bucket_bucket"),
    )
)

with_windows_accounts = (
    metrics
    .select(
        "*",
        F.avg("prev_row_seq").over(w14).alias("prev_row_seq_avg_3"),
        F.percent_rank().over(w15).alias("prev_balance_percent_rank"),
        F.avg("balance_rank_bucket").over(w16).alias("balance_rank_bucket_avg_6"),
        F.round((F.col("prev_row_seq") / F.sum("prev_row_seq").over(w13)), 4).alias("prev_row_seq_share"),
        F.dense_rank().over(w17).alias("prev_prev_balance_dense_rank"),
    )
)

scored_accounts = (
    with_windows_accounts
    .select(
        "id",
        "opened_date",
        "seq_no_bucket_2",
        "prev_row_seq",
        "prev_balance",
        "seq_no",
        "prev_prev_balance",
        "account_type_avg_balance_bucket_bucket",
        "prev_balance_percent_rank",
        "prev_prev_balance_dense_rank",
        F.lead("account_type_avg_balance_bucket_bucket", 1).over(w18).alias("next_account_type_avg_balance_bucket_bucket"),
        F.stddev_samp("balance_bucket_2").over(w19).alias("opened_date_period_stddev_samp_balance_bucket_2"),
    )
)

result = (
    scored_accounts
    .select("id", "prev_prev_balance", "prev_balance")
    .unpivot(
        ids=["id"],
        values=["prev_prev_balance", "prev_balance"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
