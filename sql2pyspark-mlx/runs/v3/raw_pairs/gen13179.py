from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id") == 7)
    .select("id", "dept_id")
    .unionByName(
        df_employees
        .filter(F.col("salary").between(89000, 130000))
        .select("id", "dept_id")
    )
    .distinct()
    .orderBy(F.col("id").desc())
)
