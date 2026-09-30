from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["OPEN", "FROZEN"]) & (F.year("opened_date") >= 2025))
    .select(
        "opened_date",
        "customer_id",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)
