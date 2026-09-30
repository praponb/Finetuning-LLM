from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "left")
    .groupBy(
        F.col("d.location"),
        F.when(F.col("d.budget") >= 4183000, "high").when(F.col("d.budget") >= 708000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct(F.col("e.salary")).alias("unique_managers"),
    )
    .filter(F.col("cnt") >= 5)
    .orderBy("location")
)
