from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(1260, 1490) & F.col("method").isNotNull())
    .orderBy(F.col("id").desc())
    .limit(5)
)
