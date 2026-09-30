from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(F.col("is_remote") & ~F.col("job_title").isin(["Engineer", "Analyst", "Director"]) & F.col("is_remote"))
)

agg = (
    valid_employees
    .groupBy("level")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.countDistinct("salary").alias("unique_managers"),
    )
)

result = (
    agg
    .select("level", "salary_range", "unique_managers")
    .orderBy(F.col("level").desc())
)
