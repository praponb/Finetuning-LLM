from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

latest_match = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.credit_limit",
        F.col("c.account_type").alias("account_account_type"),
        F.col("c.balance").alias("account_balance"),
    )
)

result = (
    latest_match
    .select("id", "email", "credit_limit", "account_account_type", "account_balance")
)
