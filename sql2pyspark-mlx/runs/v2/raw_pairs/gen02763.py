from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") <= 1320)
    .select("status")
    .intersect(df_payments.filter(F.col("method") != "card").select("status"))
    .orderBy(F.col("status").desc())
)
