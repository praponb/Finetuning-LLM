from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 152000, "high").when(F.col("salary") >= 72000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.max("salary").alias("max_salary"),
    )
    .orderBy("level_values")
)
