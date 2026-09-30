from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id") == 22)
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(
        F.countDistinct("salary").alias("unique_depts"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
    )
)
