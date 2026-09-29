from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status", "account_type").orderBy(F.col("balance").asc_nulls_last(), "id")

cleaned = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-03-27").cast("date")) & (F.col("balance") >= 43000))
    .withColumn("opened_date_label", F.date_format("opened_date", "yyyy-MM-dd"))
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"), F.col("c.merchant").alias("transaction_merchant"))
)

deduped = (
    latest_match
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

remaining_keys = (
    deduped
    .select("status", "id")
    .subtract(
        df_accounts
        .filter(F.col("balance") <= 18000)
        .select("status", "id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .filter(~F.col("d.name").like("Mia%"))
    .select("a.*", F.col("d.age"))
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "opened_date_label",
        "amount",
        "transaction_merchant",
        "age",
    )
)
