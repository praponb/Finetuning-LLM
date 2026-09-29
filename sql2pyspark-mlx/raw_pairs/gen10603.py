from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .intersect(df_suppliers.select(F.col("city").alias("status")))
    .orderBy(F.col("status").desc())
)
