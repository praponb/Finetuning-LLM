from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "level")

result = (
    df_employees
    .filter(F.col("level") == "L5")
    .select(
        "id",
        "dept_id",
        "level",
        "salary",
        F.avg("salary").over(w).alias("dept_id_avg_salary"),
    )
)
