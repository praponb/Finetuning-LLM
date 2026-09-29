from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select("email")
    .unionByName(df_employees.filter(F.col("salary") <= 79000).select("email"))
    .distinct()
    .orderBy(F.col("email").desc())
)
