from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("dept_id").orderBy("salary")

result = (
    df_employees
    .filter(F.col("salary") > 75000)
    .select(
        "id",
        "level",
        "salary",
        "dept_id",
        F.min("salary").over(w).alias("level_min_salary"),
        F.rank().over(w2).alias("salary_rank"),
    )
    .orderBy("level_min_salary")
)
