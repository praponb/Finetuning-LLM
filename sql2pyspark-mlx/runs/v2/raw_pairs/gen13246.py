from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "email", "country")
    .orderBy(F.col("id").desc())
    .limit(1)
)
