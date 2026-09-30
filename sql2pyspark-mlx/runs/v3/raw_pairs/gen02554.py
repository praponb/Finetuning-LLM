from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("level") == "L3") & ((F.col("level") == "L4") | F.col("email").isNotNull()))
    .groupBy(F.quarter("hire_date").alias("hire_quarter"), "job_title")
    .agg(
        F.countDistinct("salary").alias("unique_depts"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
    .orderBy("job_title")
)
