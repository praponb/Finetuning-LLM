from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(80000, 137000) & ~F.col("is_remote") & (F.lower(F.trim("email")) == "bob.smith@yahoo.com"))
    .select("id", "email", "hire_date")
)
