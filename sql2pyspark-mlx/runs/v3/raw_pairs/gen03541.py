from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .distinct()
    .crossJoin(df_accounts.select("account_type").distinct())
)
