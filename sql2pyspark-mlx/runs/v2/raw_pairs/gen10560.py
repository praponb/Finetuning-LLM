from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull() & F.col("name").like("%Ivy%"))
    .orderBy("name")
)
