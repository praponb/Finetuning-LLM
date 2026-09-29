from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select("name")
    .unionByName(df_customers.filter(F.col("is_active")).select("name"))
    .distinct()
)
