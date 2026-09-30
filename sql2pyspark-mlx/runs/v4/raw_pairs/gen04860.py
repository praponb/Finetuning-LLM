from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").isNotNull())
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(
        F.count("*").alias("cnt"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .filter(F.col("cnt") > 10)
    .select("txn_year", "cnt", "amount_range")
    .orderBy(F.col("cnt").desc(), F.col("txn_year").desc())
    .limit(1)
)
