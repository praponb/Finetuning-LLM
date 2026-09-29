from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_budget").desc())

result = (
    df_departments
    .groupBy(
        F.when(F.col("dept_name").isin(["Finance", "HR"]), "priority").otherwise("other").alias("dept_name_group"),
    )
    .agg(F.sum("budget").alias("total_budget"))
    .withColumn("budget_rank", F.rank().over(w))
    .orderBy("dept_name_group")
)
