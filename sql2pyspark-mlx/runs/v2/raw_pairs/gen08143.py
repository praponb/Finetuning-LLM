from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("manager_id").orderBy("hire_date")

result = (
    df_employees
    .filter(F.col("job_title") == "Designer")
    .select(
        "id",
        "level",
        "salary",
        "manager_id",
        "hire_date",
        F.avg("salary").over(w).alias("level_avg_salary"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy("level", "level_avg_salary")
)
