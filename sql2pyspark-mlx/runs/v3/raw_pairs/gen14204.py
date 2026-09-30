from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNull() & F.col("segment").isNotNull())
    .select("id", "country", "city", "age")
)
