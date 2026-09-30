from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "is_active", "name")
    .orderBy("name", "id")
    .limit(50)
)
