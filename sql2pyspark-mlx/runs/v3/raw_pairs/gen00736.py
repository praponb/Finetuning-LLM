from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([19, 21, 36]))
    .select("amount")
    .union(df_store_sales.select("revenue"))
)
