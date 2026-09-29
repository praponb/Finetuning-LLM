from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("city") == "Chiang Mai") & F.col("email").like("%@gmail.com") & F.col("email").like("%@gmail.com"))
    .orderBy("segment")
)
