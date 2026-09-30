from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([4, 6, 12]))
    .orderBy(F.col("customer_id").desc())
)
