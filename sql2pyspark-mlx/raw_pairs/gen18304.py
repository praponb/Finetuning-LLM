from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "job_title").orderBy(F.col("hire_date").desc_nulls_first(), "id")

ranked = (
    df_employees
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
    )
)

result = (
    ranked
    .groupBy(
        F.date_trunc("MONTH", "hire_date").alias("activity_month"),
        "job_title",
        F.date_trunc("YEAR", "hire_date").alias("year_start"),
    )
    .agg(
        F.count_if(F.col("job_title") == "Manager").alias("manager_events"),
        F.sum(F.when(F.col("job_title") == "Director", F.col("salary")).otherwise(0)).alias("director_salary"),
        F.count_if(F.col("job_title") == "Engineer").alias("engineer_events"),
    )
    .orderBy(F.col("year_start").desc_nulls_last())
)
