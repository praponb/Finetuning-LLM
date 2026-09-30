from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~((F.col("segment") == "Corporate") & F.col("age").between(39, 47)))
    .orderBy("city", "id")
    .limit(20)
)
