from pyspark.sql import functions as F

avg_salary = df_employees.filter(F.col("is_remote")).agg(F.avg("salary")).first()[0]

result = (
    df_employees
    .filter(F.col("salary") > avg_salary)
    .select("id", "salary")
    .orderBy("id")
)
