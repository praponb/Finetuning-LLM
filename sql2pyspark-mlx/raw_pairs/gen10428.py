from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_employees
    .groupBy("is_remote")
    .agg(F.sum("salary").alias("total_salary"))
    .withColumn("pct_of_total", F.round(((F.col("total_salary") * 100) / F.sum("total_salary").over(w)), 2))
    .orderBy("pct_of_total", "total_salary")
)
