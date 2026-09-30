from pyspark.sql import functions as F

result = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (~F.col("x.status").isin(["CLOSED", "OPEN"])), "left_anti")
)
