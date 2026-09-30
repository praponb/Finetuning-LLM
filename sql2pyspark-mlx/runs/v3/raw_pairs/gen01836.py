from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_employees
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(F.sum("salary").alias("total_salary"))
    .withColumn("pct_of_total", F.round(((F.col("total_salary") * 100) / F.sum("total_salary").over(w)), 2))
)
