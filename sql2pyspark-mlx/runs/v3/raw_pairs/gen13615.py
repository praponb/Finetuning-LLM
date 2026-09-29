from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .groupBy("is_remote")
    .agg(
        F.avg("salary").alias("avg_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
    .filter(F.col("avg_salary") > 78000)
    .orderBy("salary_range")
)
