from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "SG") & F.col("email").isNotNull())
    .select("email")
)
