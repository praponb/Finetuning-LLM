from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") <= 110000)
    .orderBy("email")
)
