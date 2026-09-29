from pyspark.sql import functions as F

src = df_transactions.filter(F.col("merchant").isin(["Starbucks", "7-Eleven", "Grab"]))

agg = (
    src
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.max("txn_time").alias("latest_txn_time"),
    )
)

result = agg.select("txn_month", "amount_range", "latest_txn_time")
