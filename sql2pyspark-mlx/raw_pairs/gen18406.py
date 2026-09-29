from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("city") == "Berlin") | F.col("email").isNotNull())
    .select("age")
)
