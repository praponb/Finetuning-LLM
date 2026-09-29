from pyspark.sql import functions as F

src = (
    df_accounts
    .filter((~F.col("account_type").isin(["SAVINGS", "LOAN", "CHECKING"]) & (F.col("status") != "FROZEN")) | (F.col("opened_date") < "2025-07-01"))
)

customer_id_summary = (
    src
    .groupBy("customer_id")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.min("opened_date").alias("first_opened_date"),
    )
)

result = (
    customer_id_summary
    .filter(F.col("avg_balance") < 37000)
    .orderBy(F.col("customer_id").desc(), "first_opened_date")
)
