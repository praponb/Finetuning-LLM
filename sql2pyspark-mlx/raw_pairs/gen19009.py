from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@gmail.com"))
    .select("id", "is_active", "city", F.upper("name").alias("name_upper"))
)
