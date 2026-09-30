from pyspark.sql import functions as F

result = (
    df_products
    .select("brand")
    .unionByName(df_transactions.select(F.col("txn_type").alias("brand")))
    .orderBy("brand")
)
