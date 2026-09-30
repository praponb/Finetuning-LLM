from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("email") >= 5)
    .select("hire_date")
    .unionByName(df_employees.filter(F.col("salary") >= 64000).select("hire_date"))
    .distinct()
    .orderBy("hire_date")
)
