from pyspark.sql import functions as F

result = (
    df_payments
    .select("method")
    .intersect(df_customers.select("country"))
    .orderBy(F.col("method").desc())
)
