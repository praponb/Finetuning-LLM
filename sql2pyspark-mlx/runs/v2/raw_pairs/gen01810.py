from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "SHIPPED")
    .select("status")
    .subtract(df_accounts.select("status"))
    .orderBy("status")
)
