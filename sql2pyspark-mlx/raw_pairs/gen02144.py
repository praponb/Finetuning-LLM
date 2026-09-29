from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select(F.lit("customer").alias("record_type"), "email")
    .unionByName(df_employees.select(F.lit("employee").alias("record_type"), "email"))
    .distinct()
)
