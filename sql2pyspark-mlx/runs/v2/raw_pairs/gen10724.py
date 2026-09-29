from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(90000, 105000) | F.col("level").isin(["L2", "L3"]))
    .groupBy(
        F.when(F.col("salary") >= 138000, "high").when(F.col("salary") >= 81000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.max("hire_date").alias("last_hire_date"))
)
