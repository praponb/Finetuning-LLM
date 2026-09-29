from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") <= 89000) & F.col("salary").between(116000, 144000))
    .select("id", "full_name", "dept_id")
    .orderBy(F.col("full_name").desc())
)
