from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower("email").like("%aro%"))
    .select("id", "is_active", "city", "segment")
    .orderBy("id")
    .limit(20)
)
