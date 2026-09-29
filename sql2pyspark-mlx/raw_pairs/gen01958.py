from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("is_remote") & (F.col("level") == "L3")) | F.col("job_title").isNull())
    .groupBy(
        F.when(F.col("salary") >= 159000, "high").when(F.col("salary") >= 41000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.sort_array(F.collect_set("level")).alias("level_list"))
    .orderBy(F.col("salary_bucket").desc())
)
