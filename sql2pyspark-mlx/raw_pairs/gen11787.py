from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .groupBy("level")
    .agg(
        F.max("salary").alias("max_salary"),
        F.countDistinct("salary").alias("unique_depts"),
        F.min("salary").alias("min_salary"),
    )
)
