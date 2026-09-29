from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "age", "country")
    .orderBy(F.col("age").desc(), "id")
    .limit(20)
)
