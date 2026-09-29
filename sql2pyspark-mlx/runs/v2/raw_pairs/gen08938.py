from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").isNull())
    .select("id", "segment", "name", "is_active")
    .orderBy("segment")
)
