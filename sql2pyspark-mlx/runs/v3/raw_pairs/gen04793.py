from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isNotNull() & F.month("hire_date").isin([3, 6, 10]))
)
