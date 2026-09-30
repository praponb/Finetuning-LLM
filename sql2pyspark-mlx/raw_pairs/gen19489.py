from pyspark.sql import functions as F

result = (
    df_orders
    .select("channel")
    .distinct()
    .crossJoin(df_accounts.select("status").distinct())
    .orderBy("channel", "status")
)
