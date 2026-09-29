from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .filter(F.col("avg_salary") > 72)
    .orderBy(F.col("level").desc(), "avg_salary")
)
