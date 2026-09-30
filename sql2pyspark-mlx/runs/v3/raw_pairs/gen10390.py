from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id").isin([5, 29, 40]) & F.col("customer_id").isNotNull()) | (F.col("customer_id") == 29))
    .orderBy(F.col("customer_id").desc(), "balance", "id")
    .limit(50)
)
