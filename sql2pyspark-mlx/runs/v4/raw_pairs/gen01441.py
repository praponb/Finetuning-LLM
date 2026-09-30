from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy("hire_date", "id").rowsBetween(-2, -1)
w2 = Window.orderBy(F.col("salary").desc(), "id")
w3 = Window.orderBy("hire_date", "id")
w4 = Window.partitionBy("manager_id")
w5 = Window.partitionBy("manager_id", "dept_id")

result = (
    df_employees
    .select(
        "*",
        F.avg("salary").over(w).alias("salary_avg_2_prior"),
        F.ntile(5).over(w2).alias("salary_bucket"),
        F.lead("salary", 1).over(w3).alias("next_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w4)), 4).alias("salary_share"),
        F.max("salary").over(w5).alias("manager_id_max_salary"),
    )
)
