from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("level").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(~F.col("full_name").like("%Mia%"))
    .select(
        "id",
        "level",
        "salary",
        "hire_date",
        F.min("salary").over(w).alias("level_min_salary"),
        (F.col("salary") - F.lag("salary", 1).over(w2)).alias("salary_change"),
    )
    .orderBy("level", F.col("level_min_salary").desc())
)
