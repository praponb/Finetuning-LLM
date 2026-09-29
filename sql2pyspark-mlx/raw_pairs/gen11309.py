from pyspark.sql import functions as F

result = (
    df_transactions
    .select("txn_type")
    .union(df_customers.filter(F.col("segment").isNotNull()).select("segment"))
    .distinct()
)
