from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["SG", "TH", "DE"]))
    .select("signup_date", "name")
    .unionByName(df_customers.filter(F.col("age") >= 56).select("signup_date", "name"))
    .distinct()
    .orderBy("name")
)
