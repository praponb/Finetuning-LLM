from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") <= "2023-03-01")
    .select("id", "age")
    .unionByName(df_customers.filter(F.length("name") > 14).select("id", "age"))
    .distinct()
)
