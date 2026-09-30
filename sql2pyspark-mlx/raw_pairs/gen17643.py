from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy(F.year("hire_date"), "manager_id").orderBy(F.col("salary").desc(), "id")

with_windows = (
    df_employees
    .select(
        "*",
        F.avg("salary").over(w).alias("manager_id_avg_salary"),
        F.avg("salary").over(w2).alias("salary_avg_5_prior"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.when(F.col("level").rlike("^(L4|L2)$"), "GROUP_A").when(F.col("level").rlike("^(L1|L3)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("level_zone"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
