from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") > 1130)
    .select("status")
    .subtract(df_payments.filter(F.col("amount") >= 1170).select("status"))
)
