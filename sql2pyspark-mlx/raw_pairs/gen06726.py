from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_last = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"), F.col("c.merchant"))
)

result = (
    with_last
    .withColumn("quarter_start", F.date_trunc("quarter", "opened_date"))
    .groupingSets(
        [["quarter_start", "customer_id"], ["quarter_start"]],
        "quarter_start",
        "customer_id",
    )
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
)
