from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-05-28").cast("date"))
    .withColumn("email_filled", F.coalesce("email", F.lit("unassigned")))
    .withColumn("salary_int", F.col("salary").cast("int"))
)

result = (
    cleaned
    .withColumn("year_start", F.date_trunc("YEAR", "hire_date"))
    .withColumn("salary_tier", F.when(F.col("salary") >= 162000, "LARGE").otherwise("SMALL"))
    .cube("year_start", "salary_tier")
    .agg(
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
        F.sum(F.when(F.col("job_title") == "Analyst", F.col("salary_int")).otherwise(0)).alias("analyst_salary_int"),
        F.round(F.avg("salary_int"), 2).alias("avg_salary_int"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.grouping("year_start").alias("grouping_id"),
    )
)
