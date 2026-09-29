from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.count("*").alias("cnt"),
    )
)
