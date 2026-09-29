from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").asc_nulls_last(), "id")

deduped = (
    df_employees
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "full_name", "dept_id", "manager_id", "hire_date")
)

result = (
    deduped
    .cube(F.date_trunc("year", "hire_date").alias("year_start"))
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("dept_id").alias("distinct_dept_ids"),
        F.countDistinct("dept_id").alias("distinct_dept_ids_2"),
    )
)
