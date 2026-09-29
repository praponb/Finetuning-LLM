from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w_part = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(-5, -1)
w3 = Window.orderBy(F.col("salary").desc(), "id")

filtered = (
    df_employees
    .filter((~F.col("job_title").eqNullSafe(F.lit("Analyst"))) & F.col("manager_id").isNotNull())
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
    .withColumn("full_name_filled", F.coalesce("full_name", F.lit("N/A")))
)

result = (
    filtered
    .withColumn("manager_id_max_salary", F.max("salary").over(w))
    .withColumn("salary_avg_5_prior", F.avg("salary").over(w_part))
    .withColumn("event_seq", F.row_number().over(w3))
)
