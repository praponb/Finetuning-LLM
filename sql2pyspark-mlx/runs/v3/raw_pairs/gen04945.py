from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "UK")
    .select("email", "name")
    .unionByName(df_customers.filter(F.length("name") > 9).select("email", "name"))
    .orderBy("email")
)
