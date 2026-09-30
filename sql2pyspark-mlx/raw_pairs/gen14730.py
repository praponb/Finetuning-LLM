from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .groupBy("job_title")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_salary") * 100) / F.sum("total_salary").over(w)), 2))
    .orderBy(F.col("total_salary").desc())
)
