from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") >= 57000) | F.col("full_name").isNotNull())
    .groupBy(
        "job_title",
        F.when(F.col("job_title").isin(["Engineer", "Analyst"]), "core").otherwise("other").alias("job_title_group"),
    )
    .agg(
        F.min("salary").alias("min_salary"),
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.sum(F.when(F.col("job_title") == "Engineer", F.col("salary")).otherwise(0)).alias("engineer_salary"),
    )
    .filter(F.col("engineer_salary") < 22)
    .drop("engineer_salary")
    .orderBy(F.col("l5_count").desc())
)
