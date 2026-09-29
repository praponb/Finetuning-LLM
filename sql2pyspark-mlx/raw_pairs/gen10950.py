from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id")
w2 = Window.partitionBy("customer_id", "status").orderBy(F.col("next_balance").asc_nulls_last(), "id")
w3 = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_last())
w4 = Window.partitionBy("customer_id").orderBy(F.col("next_balance").desc_nulls_last(), "id")

enriched = (
    df_accounts
    .select(
        "*",
        F.lead("balance").over(w).alias("next_balance"),
        F.lag("balance").over(w).alias("prev_balance"),
        F.row_number().over(w).alias("row_seq"),
    )
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        "next_balance",
        "prev_balance",
        "row_seq",
    )
)

ranked_accounts = (
    ranked
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

not_flagged = (
    ranked_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.amount") > 2500), "left_anti")
)

result = (
    not_flagged
    .cube(F.date_trunc("QUARTER", "opened_date").alias("quarter_start"))
    .agg(
        F.count("*").alias("row_count"),
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
)
