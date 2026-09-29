from pyspark.sql import functions as F

result = (
    df_customers
    .select("email", "is_active", "city")
    .orderBy(F.col("city").desc())
)
