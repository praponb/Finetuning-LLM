from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "manager_id").orderBy(F.col("salary").desc(), "id")

latest = (
    df_employees
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("QUARTER", "hire_date").alias("quarter_start"), "manager_id")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("level") == "L1", 1)), F.lit(0)), 2).alias("salary_per_l1"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list_2"),
    )
    .filter(F.col("salary_per_l1") >= 0)
)
