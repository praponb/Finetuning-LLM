from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "segment")
    .orderBy("segment", "id")
    .limit(5)
)
