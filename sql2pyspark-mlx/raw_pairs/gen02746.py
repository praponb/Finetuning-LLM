from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(F.col("is_remote") & F.col("dept_id").isin([4, 7]) & (F.col("salary") < 66000))
)

agg = (
    valid_employees
    .groupBy("manager_id")
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
)

result = agg
