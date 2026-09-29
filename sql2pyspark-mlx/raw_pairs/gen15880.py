from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter(F.month("hire_date").isin([1, 2, 9]) & (F.col("salary") <= 140000))
)

agg = (
    recent_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.sum("salary").alias("total_salary"),
        F.min("hire_date").alias("min_hire_date"),
    )
)

result = (
    agg
    .filter(F.col("total_salary") > 634000)
    .select("hire_quarter", "min_hire_date")
)
