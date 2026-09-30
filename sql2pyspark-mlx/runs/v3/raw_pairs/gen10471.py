from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("manager_id").isin([11, 26]))
    .withColumn("full_name_upper", F.upper("full_name"))
)

is_remote_summary = (
    base
    .groupBy("is_remote")
    .agg(F.avg("salary").alias("avg_salary"))
)

result = is_remote_summary
