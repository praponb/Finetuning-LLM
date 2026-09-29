from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") != "cash")
    .select("method")
    .unionByName(df_payments.filter(F.col("amount") < 710).select("method"))
    .distinct()
    .orderBy("method")
)
