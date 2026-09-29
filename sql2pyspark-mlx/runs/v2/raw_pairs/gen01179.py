from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 710)
    .select("method", "amount", "status")
    .orderBy("method")
)
