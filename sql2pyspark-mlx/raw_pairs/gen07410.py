from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "customer_id").orderBy("age")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy(F.month("opened_date"), "status").orderBy("opened_date", "id")
w4 = Window.partitionBy(F.date_trunc("YEAR", "opened_date"), "status").orderBy("opened_date", "id")
w5 = Window.partitionBy("status").orderBy("age_share", "id")
w_recent = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy(F.date_trunc("WEEK", "opened_date"))

base = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "LOAN", "SAVINGS"]) & F.col("status").isNotNull())
    .select("*", F.trim("account_type").alias("clean_account_type"))
)

not_flagged = (
    base
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.merchant").isin(["Lazada", "Shell", "7-Eleven"])), "left_semi")
)

enriched = (
    not_flagged
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.segment"), F.col("p.age"), F.col("p.email"))
)

ranked = (
    enriched
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "opened_date", "status", "clean_account_type", "age", "email")
)

with_windows = (
    ranked
    .select(
        "*",
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
        F.lead("age", 2).over(w3).alias("next_age"),
        F.lag("age", 2).over(w4).alias("prev_age"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_anti")
)

enriched_accounts = (
    eligible
    .select(
        "id",
        "opened_date",
        "email",
        "age_share",
        "prev_age",
        F.lead("next_age", 1).over(w5).alias("next_next_age"),
        F.avg("next_age").over(w_recent).alias("next_age_avg_3"),
        F.round((F.col("prev_age") / F.sum("prev_age").over(w7)), 4).alias("prev_age_share"),
    )
)

result = (
    enriched_accounts
    .select("id", "opened_date", "email", "age_share", "next_next_age")
    .orderBy(F.col("opened_date").desc())
)
