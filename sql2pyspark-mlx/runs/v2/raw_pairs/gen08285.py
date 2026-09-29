from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "DE")
    .select("id", "name")
    .unionByName(df_customers.filter(F.col("city").isNotNull()).select("id", "name"))
    .orderBy(F.col("name").desc())
)
