from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("amount") < 520) & (F.col("status") == "SUCCESS")) | (F.col("status") == "REFUNDED"))
)
