from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 132000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.max("salary").alias("max_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
