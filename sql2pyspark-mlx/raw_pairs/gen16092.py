from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id").orderBy("opened_date", "id")
w2 = Window.orderBy("opened_date", "id").rowsBetween(-3, -1)

enriched = (
    df_accounts
    .select(
        "id",
        "account_type",
        "balance",
        F.lead("balance").over(w).alias("next_balance"),
        F.lag("balance").over(w).alias("prev_balance"),
        F.row_number().over(w).alias("seq_no"),
        F.avg("balance").over(w2).alias("balance_avg_3_prior"),
    )
)

result = (
    enriched
    .groupBy("seq_no")
    .agg(
        F.count_if(F.col("account_type") == "LOAN").alias("loan_events"),
        F.percentile_approx("balance_avg_3_prior", 0.95).alias("balance_avg_3_prior_p95"),
        F.count("*").alias("event_count"),
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("account_type") == "LOAN", 1)), F.lit(0)), 2).alias("seq_no_per_loan"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("balance_avg_3_prior_p95") >= 0)
    .orderBy(F.col("seq_no_per_loan").desc_nulls_first())
)
