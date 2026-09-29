from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 149000, "high").when(F.col("salary") >= 61000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.sum("salary").alias("total_salary"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_salary") * 100) / F.sum("total_salary").over(w)), 2))
    .orderBy("salary_bucket")
)
