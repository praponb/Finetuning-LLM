from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("signup_date").desc(), "id")

enriched = (
    df_accounts
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.credit_limit"), F.col("p.name"), F.col("p.signup_date"))
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

kept = (
    latest
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.amount") <= 1900), "left_semi")
)

result = (
    kept
    .select("id", "status", "balance", F.col("credit_limit").alias("CREDIT_LIMIT_VAL"))
    .unpivot(
        ["id", "status"],
        ["balance", "CREDIT_LIMIT_VAL"],
        "metric_name",
        "metric_value",
    )
)
