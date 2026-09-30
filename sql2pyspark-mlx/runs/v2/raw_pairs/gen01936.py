from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isNull())
    .select("city")
    .intersect(df_customers.filter(F.col("credit_limit") <= 6600).select("city"))
    .orderBy(F.col("city").desc())
)
