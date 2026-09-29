from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "city", "email", "is_active", F.round("age", 2).alias("age_rounded"))
    .orderBy("id")
    .limit(5)
)
