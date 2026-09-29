from pyspark.sql import functions as F

result = (
    df_departments
    .select(F.lit("department").alias("origin"), "location")
    .union(df_customers.select(F.lit("customer").alias("origin"), "city"))
    .distinct()
    .orderBy("origin")
)
