from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")
w2 = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc(), "id")
w_part = Window.orderBy("hire_date", "id").rowsBetween(-2, -1)
w_ordered = Window.orderBy("row_seq_2", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2024-02-01").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "hire_date",
        "is_remote",
        F.initcap(F.trim("level")).alias("level_title"),
        F.concat("job_title", F.lit(" "), "full_name").alias("job_title_full_name"),
        F.coalesce("level", F.lit("DIRECT")).alias("resolved_level"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
    )
)

scored = (
    with_windows
    .withColumn("row_seq_avg_2_prior", F.avg("row_seq").over(w_part))
    .withColumn("row_seq_2_avg_3", F.avg("row_seq_2").over(w_ordered))
)

with_dims = (
    scored
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.level"),
        F.col("p.full_name").alias("employee_full_name"),
        F.col("p.email").alias("employee_email"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "hire_date",
        "job_title_full_name",
        "row_seq",
        "row_seq_2",
        "row_seq_avg_2_prior",
    )
)
