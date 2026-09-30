from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("id", "email")
    .unionByName(df_customers.filter(~F.col("is_active")).select("id", "email"))
    .distinct()
    .orderBy("id")
)
