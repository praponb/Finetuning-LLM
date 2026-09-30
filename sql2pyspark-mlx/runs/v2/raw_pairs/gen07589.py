from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNull())
    .withColumn("salary_rounded", F.round("salary", 2))
    .withColumn("salary_int", F.col("salary").cast("int"))
)
