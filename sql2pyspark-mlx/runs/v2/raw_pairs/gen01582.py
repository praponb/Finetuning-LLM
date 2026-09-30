from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.lower(F.trim("full_name")) == "kanya srisuk")
    .groupBy(
        "level",
        F.when(F.col("salary") >= 123000, "high").when(F.col("salary") >= 57000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.sum("salary").alias("total_salary"))
    .orderBy("level", "salary_bucket")
    .limit(10)
)
