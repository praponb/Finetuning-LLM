from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .distinct()
    .crossJoin(df_payments.select("method").distinct())
    .orderBy("status", "method")
)
