from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_salary").desc())

result = (
    df_employees
    .filter(F.col("job_title") != "Manager")
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(F.sum("salary").alias("total_salary"))
    .withColumn("rnk", F.rank().over(w))
)
