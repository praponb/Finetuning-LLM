from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(510, 560))
    .select("method")
    .distinct()
)
