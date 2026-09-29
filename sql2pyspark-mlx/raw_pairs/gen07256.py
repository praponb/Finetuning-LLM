from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w_running = Window.partitionBy("dept_id").orderBy("rank_in_group", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("manager_id").orderBy("rank_in_group", "id")
w4 = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc_nulls_first(), "id")

filtered = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (~F.col("level").eqNullSafe(F.lit("L5"))))
    .withColumn("full_name_filled", F.coalesce("full_name", F.lit("unassigned")))
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked
    .select(
        "*",
        F.max("salary").over(w_running).alias("last7_max_salary"),
        F.count("salary").over(w_running).alias("last7_count_salary"),
        F.sum("salary").over(w_running).alias("last7_sum_salary"),
        F.lead("salary", 1).over(w3).alias("next_salary"),
    )
)

latest = (
    metrics
    .withColumn("rank_in_group_2", F.row_number().over(w4))
    .filter(F.col("rank_in_group_2") == 1)
)

result = (
    latest
    .groupBy("level")
    .agg(
        F.sum(F.when(F.col("job_title") == "Analyst", F.col("last7_count_salary"))).alias("analyst_total_last7_count_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
    .orderBy("analyst_total_last7_count_salary")
)
