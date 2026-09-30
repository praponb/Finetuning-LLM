from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.lower("email").like("%ob.%"))
    .groupBy("is_remote")
    .agg(
        F.count("*").alias("cnt"),
        F.min("salary").alias("min_salary"),
    )
    .filter(F.col("min_salary") > 96000)
    .drop("min_salary")
)
