from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(132000, 149000) & F.col("email").like("%@startup.io"))
    .select("salary")
    .orderBy(F.col("salary").desc())
)
