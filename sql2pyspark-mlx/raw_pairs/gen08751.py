from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "OPEN") & (F.col("status").isin(["OPEN", "FROZEN", "CLOSED"]) | ~F.col("status").isin(["FROZEN", "CLOSED"])))
    .groupBy("status")
    .agg(
        F.max("balance").alias("max_balance"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .orderBy("status", "first_opened_date")
)
