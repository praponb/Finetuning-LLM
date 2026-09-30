from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "Consumer")
    .select("age")
    .intersect(df_customers.filter(F.col("is_active")).select("age"))
    .orderBy("age")
)
