from pyspark.sql import functions as F

src = df_transactions.filter(F.col("txn_type") == "CREDIT")

agg = (
    src
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_count"),
        F.count("*").alias("cnt"),
    )
)

result = agg.select("txn_year", "debit_count", "cnt")
