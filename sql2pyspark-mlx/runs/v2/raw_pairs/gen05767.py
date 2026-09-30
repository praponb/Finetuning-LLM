from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(94000, 118000) & F.col("manager_id").isin([22, 23]) & F.col("email").isNotNull())
    .orderBy("full_name")
)
