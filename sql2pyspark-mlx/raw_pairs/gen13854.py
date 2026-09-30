from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w2 = Window.partitionBy(F.month("hire_date_plus_months"), "dept_id").orderBy(F.col("hire_date_plus_months").desc(), "id")
w3 = Window.partitionBy(F.year("hire_date_plus_months"), "level").orderBy("hire_date_plus_months", "id")
w_recent = Window.partitionBy("manager_id").orderBy("prev_salary", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("manager_id").orderBy("manager_id_avg_salary", "id").rowsBetween(-6, -1)

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-06-23").cast("date")) & F.col("job_title").isNotNull())
    .select("*", F.add_months("hire_date", 1).alias("hire_date_plus_months"))
)

with_windows = (
    valid
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "is_remote",
        "level",
        F.avg("salary").over(w).alias("manager_id_avg_salary"),
        F.row_number().over(w2).alias("row_seq"),
        F.lag("salary", 1, 0).over(w3).alias("prev_salary"),
    )
)

enriched = (
    with_windows
    .withColumn("last3_max_manager_id_avg_salary", F.max("manager_id_avg_salary").over(w_recent))
    .withColumn("last3_min_manager_id_avg_salary", F.min("manager_id_avg_salary").over(w_recent))
    .withColumn("last3_avg_manager_id_avg_salary", F.avg("manager_id_avg_salary").over(w_recent))
    .withColumn("row_seq_avg_6_prior", F.avg("row_seq").over(w5))
)

base = (
    enriched
    .filter(F.col("level").isNotNull())
    .withColumn("clean_email", F.trim("email"))
)

result = (
    base
    .groupBy("manager_id", "level")
    .agg(
        F.round(F.sum("last3_max_manager_id_avg_salary") / F.nullif(F.count(F.when(F.col("level") == "L5", 1)), F.lit(0)), 2).alias("last3_max_manager_id_avg_salary_per_l5"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
        F.avg("last3_max_manager_id_avg_salary").alias("avg_last3_max_manager_id_avg_salary"),
        F.sum(F.when(F.col("level") == "L4", F.col("last3_min_manager_id_avg_salary")).otherwise(0)).alias("l4_last3_min_manager_id_avg_salary"),
    )
    .filter(F.col("last3_max_manager_id_avg_salary_per_l5") > 0)
    .orderBy(F.col("l1_count").desc())
)
