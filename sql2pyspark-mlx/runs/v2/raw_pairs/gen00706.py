from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Che%"))
    .select("id", "is_active", "city", "credit_limit")
)
