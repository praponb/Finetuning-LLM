from pyspark.sql import functions as F

result = (
    df_transactions
    .select("txn_type")
    .intersect(df_employees.select(F.col("full_name").alias("txn_type")))
    .orderBy("txn_type")
)
