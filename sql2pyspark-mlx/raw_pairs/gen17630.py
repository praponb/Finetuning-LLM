from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")

cleaned = (
    df_accounts
    .filter(F.col("balance") >= 55000)
    .withColumn("account_type_lower", F.lower("account_type"))
    .withColumn("status_filled", F.coalesce("status", F.lit("OTHER")))
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "account_type")
)

cleaned_accounts = (
    latest
    .filter(F.col("account_type") == "SAVINGS")
    .select(
        "*",
        F.trim("account_type").alias("clean_account_type"),
        F.upper("account_type").alias("account_type_upper"),
    )
)

kept = (
    cleaned_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.amount").isNotNull()), "left_anti")
)

result = (
    kept
    .select("id", "account_type", "clean_account_type", "account_type_upper")
    .orderBy(F.col("id").asc_nulls_first(), "account_type")
)
