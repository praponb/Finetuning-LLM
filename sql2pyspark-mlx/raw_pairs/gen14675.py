from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id")
w2 = Window.partitionBy("manager_id").orderBy(F.col("hire_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy(F.year("hire_date"), "job_title").orderBy("salary", "id")
w4 = Window.partitionBy("job_title")

cleaned = (
    df_employees
    .filter((F.upper(F.trim("email")) == "EMMA@OUTLOOK.COM") & (~F.col("level").eqNullSafe(F.lit("L4"))))
    .withColumn("hire_date_period", F.date_trunc("month", "hire_date"))
)

result = (
    cleaned
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "level",
        "hire_date_period",
        F.row_number().over(w).alias("row_seq"),
        F.avg("salary").over(w2).alias("salary_avg_7d"),
        F.row_number().over(w3).alias("event_seq"),
        F.round((F.col("salary") / F.sum("salary").over(w4)), 4).alias("salary_share"),
    )
)
