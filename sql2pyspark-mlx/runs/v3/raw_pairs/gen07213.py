from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.month("hire_date").isin([5, 7, 12]))
    .orderBy("full_name", "id")
    .limit(10)
)
