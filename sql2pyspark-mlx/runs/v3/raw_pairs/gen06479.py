from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@mail.co.th") & (F.col("is_remote") | F.col("full_name").isNotNull()))
    .rollup(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
        "job_title",
    )
    .agg(F.sort_array(F.collect_set("level")).alias("level_list"))
    .orderBy("hire_cohort", F.col("job_title").desc())
)
