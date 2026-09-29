from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .subtract(df_accounts.select("status"))
    .orderBy(F.col("status").desc())
)
