from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("job_title").isin(["Engineer", "Analyst"]), "group_a").otherwise("other").alias("job_title_group"),
        "level",
    )
    .agg(F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_count"))
)
