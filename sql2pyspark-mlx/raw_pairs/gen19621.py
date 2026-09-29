from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("hire_date")).orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy(F.date_trunc("YEAR", "hire_date"), "manager_id").orderBy("hire_date", "id")
w3 = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc(), "id")
w4 = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())
w5 = Window.partitionBy("dept_id")

result = (
    df_employees
    .select(
        "id",
        "email",
        "dept_id",
        "salary",
        "hire_date",
        "is_remote",
        F.ntile(3).over(w).alias("salary_bucket"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("event_seq_2"),
        F.cume_dist().over(w4).alias("salary_cume_dist"),
        F.round((F.col("salary") / F.sum("salary").over(w5)), 4).alias("salary_share"),
    )
)
