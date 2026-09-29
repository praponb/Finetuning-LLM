from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(75000, 109000))
    .select("hire_date")
    .intersect(df_employees.filter(F.col("is_remote")).select("hire_date"))
    .orderBy("hire_date")
)
