from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(680, 1040) | F.col("method").isNotNull())
    .orderBy(F.col("method").desc())
)
