from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~(~F.col("is_active") & F.col("credit_limit").between(8200, 11400)))
    .orderBy(F.col("city").desc())
)
