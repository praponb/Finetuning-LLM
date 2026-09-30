from pyspark.sql import functions as F

recent_employees = df_employees.filter(~F.col("is_remote"))

summary = (
    recent_employees
    .groupBy("is_remote")
    .agg(F.countDistinct("salary").alias("unique_levels"))
)

result = summary.orderBy("unique_levels")
