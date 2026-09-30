from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("salary", "id")

metrics = (
    df_employees
    .select(
        "id",
        "full_name",
        "manager_id",
        "hire_date",
        "is_remote",
        "level",
        F.sum("salary").over(w).alias("running_salary"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    metrics
    .groupBy(F.date_trunc("YEAR", "hire_date").alias("year_start"))
    .agg(
        F.percentile_approx("seq_no", 0.95).alias("seq_no_p95"),
        F.round(F.sum("running_salary") / F.nullif(F.count(F.when(F.col("level") == "L1", 1)), F.lit(0)), 2).alias("running_salary_per_l1"),
        F.sum(F.when(F.col("level") == "L3", F.col("seq_no")).otherwise(0)).alias("l3_seq_no"),
        F.percentile_approx("running_salary", [0.25, 0.5, 0.75]).alias("running_salary_quartiles"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("seq_no_p95") >= 0)
)
