from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower(F.trim("name")) == "olivia garcia")
    .select("email")
    .unionByName(df_employees.select("email"))
    .distinct()
    .orderBy(F.col("email").desc())
)
