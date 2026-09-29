from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(103000, 117000) & (F.upper(F.trim("email")) == "GRACE.KIM@GMAIL.COM") & (F.col("dept_id") == 12))
    .orderBy("hire_date", "id")
    .limit(10)
)
