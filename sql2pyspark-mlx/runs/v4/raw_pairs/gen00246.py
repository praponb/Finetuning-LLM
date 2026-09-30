from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([3, 4, 6]))
    .select("revenue", F.lit("store_sale").alias("source"))
    .union(df_transactions.select("amount", F.lit("transaction").alias("source")))
    .distinct()
)
