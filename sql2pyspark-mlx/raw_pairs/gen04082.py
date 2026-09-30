from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([6, 8, 13]) & F.col("dept_id").isin([1, 5]))
    .withColumn("full_name_level", F.concat("full_name", F.lit(" - "), "level"))
)
