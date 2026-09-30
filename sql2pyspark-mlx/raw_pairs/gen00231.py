from pyspark.sql import functions as F

result = (
    df_payments
    .select("method")
    .intersect(df_customers.filter(F.col("is_active")).select("segment"))
)
