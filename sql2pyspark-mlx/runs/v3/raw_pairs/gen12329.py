from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "opened_date",
        F.ceil("balance").alias("balance_whole"),
        F.when(F.col("status").isin(["OPEN", "CLOSED"]), "Tier 1").otherwise("Other").alias("status_group"),
    )
    .orderBy("id")
)
