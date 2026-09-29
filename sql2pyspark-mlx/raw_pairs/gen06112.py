from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-04-26").cast("date")) & F.col("merchant").isNotNull())
    .withColumn("txn_time_date", F.to_date("txn_time"))
)

result = (
    valid
    .select("txn_type", "merchant")
    .intersectAll(
        df_transactions
        .filter(F.col("txn_time") < "2023-03-01")
        .select("txn_type", "merchant")
    )
)
