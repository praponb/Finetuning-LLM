from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull())
    .select("country", "name")
    .unionByName(df_customers.filter(F.length("name") > 9).select("country", "name"))
    .distinct()
)
