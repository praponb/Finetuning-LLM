from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("Ol%"))
    .select("email")
    .unionByName(df_employees.filter(F.col("dept_id") == 31).select("email"))
    .distinct()
    .orderBy("email")
)
