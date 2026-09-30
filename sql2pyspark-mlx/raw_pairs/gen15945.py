from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_cohort")

result = (
    df_employees
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(
        F.sum("salary").alias("total_salary"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_salary", F.sum("total_salary").over(w))
    .orderBy(F.col("hire_cohort").desc())
    .limit(10)
)
