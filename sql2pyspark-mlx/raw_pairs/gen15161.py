from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~(F.month("hire_date").isin([2, 6, 12]) & F.col("dept_id").isin([8, 17]) & (F.col("salary") <= 107000)))
    .withColumn("hire_date_weekofyear", F.weekofyear("hire_date"))
    .withColumn("salary_int", F.col("salary").cast("int"))
)
