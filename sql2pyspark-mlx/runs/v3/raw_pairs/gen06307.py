from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") != 63)
    .select("name", "city")
    .unionByName(df_customers.filter(F.col("country") != "SG").select("name", "city"))
    .orderBy(F.col("name").desc())
)
