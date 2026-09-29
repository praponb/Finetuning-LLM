from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc_nulls_last(), "id")

latest = (
    df_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("year", "hire_date").alias("year_start"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("job_title") == "Intern", F.col("salary"))).alias("intern_total_salary"),
        F.count_if(F.col("level") == "L4").alias("l4_events"),
        F.max(F.when(F.col("level") == "L1", F.col("salary"))).alias("l1_max_salary"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_rows"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
    .filter(F.col("l4_events") > 1)
)
