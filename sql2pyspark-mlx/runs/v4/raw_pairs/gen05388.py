from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary")
w2 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("salary", "id")
w4 = Window.partitionBy("manager_id")

base = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-06-06"))) & F.col("job_title").isNotNull() & (F.col("hire_date") <= "2024-01-29"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "is_remote",
        "level",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)

deduped = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_dims = (
    deduped
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.email").alias("employee_email"), F.col("p.job_title"))
)

result = (
    with_dims
    .select(
        "*",
        F.avg("salary").over(w2).alias("salary_avg_5"),
        F.row_number().over(w3).alias("row_seq"),
        F.stddev_samp("salary").over(w4).alias("manager_id_stddev_samp_salary"),
    )
)
