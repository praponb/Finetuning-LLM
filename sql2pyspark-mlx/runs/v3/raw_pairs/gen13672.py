from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L1")
    .groupBy("is_remote")
    .agg(F.countDistinct("salary").alias("unique_managers"))
    .filter(F.col("unique_managers") >= 4)
)
