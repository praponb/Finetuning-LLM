from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNull())
    .groupBy(
        F.when(F.col("salary") >= 150000, "high").when(F.col("salary") >= 43000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
    .orderBy(F.col("avg_salary").desc())
)
