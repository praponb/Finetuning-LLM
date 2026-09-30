from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("job_title_group")

result = (
    df_employees
    .filter(F.col("salary") <= 103000)
    .groupBy(
        F.when(F.col("job_title").isin(["Engineer", "Director"]), "group_a").otherwise("other").alias("job_title_group"),
    )
    .agg(F.sum("salary").alias("total_salary"))
    .withColumn("cumulative_salary", F.sum("total_salary").over(w))
    .orderBy("job_title_group", F.col("cumulative_salary").desc())
)
