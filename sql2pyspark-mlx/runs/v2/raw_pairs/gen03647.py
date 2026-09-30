from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["CLOSED", "OPEN"]) & (F.col("balance") > 52000) & (F.col("status") == "CLOSED"))
    .select(
        "id",
        "customer_id",
        "opened_date",
        F.initcap(F.trim("account_type")).alias("account_type_title"),
    )
    .orderBy(F.col("id").desc())
)
