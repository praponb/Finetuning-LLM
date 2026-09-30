from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("age")
    .unionByName(df_customers.filter(F.col("segment") == "Corporate").select("age"))
    .orderBy(F.col("age").desc())
)
