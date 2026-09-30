from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") < 44)
    .select("country")
    .distinct()
    .orderBy("country")
)
