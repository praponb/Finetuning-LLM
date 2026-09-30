from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc_nulls_last(), "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

joined = (
    df_accounts
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.email"))
)

cleaned = (
    joined
    .filter(~F.col("account_type").isin(["CREDIT", "LOAN"]))
    .withColumn("account_type_group", F.when(F.col("account_type").isin(["CHECKING", "CREDIT"]), "Core").otherwise("Other"))
    .withColumn("opened_date_label", F.date_format("opened_date", "MMM yyyy"))
    .withColumn("status_filled", F.coalesce("status", F.lit("DIRECT")))
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "account_type_group",
        "opened_date_label",
        "status_filled",
        F.col("min_account_balance"),
    )
    .orderBy(F.col("min_account_balance").asc_nulls_last(), "status")
)
