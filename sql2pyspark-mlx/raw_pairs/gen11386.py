from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull())
    .select("city")
    .unionByName(df_stores.select("city"))
    .distinct()
    .orderBy(F.col("city").desc())
)
