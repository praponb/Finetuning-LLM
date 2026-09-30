from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "dept_id").orderBy("salary", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("manager_id")
w3 = Window.orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy("manager_id").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.upper(F.trim("email")) == "HENRY@STARTUP.IO")
    .select(
        "*",
        F.avg("salary").over(w).alias("salary_avg_6_prior"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.ntile(4).over(w3).alias("salary_bucket"),
        F.row_number().over(w4).alias("event_seq"),
    )
)
