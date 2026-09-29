from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("full_name").like("Em%"))
    .select(
        "id",
        "dept_id",
        "salary",
        "level",
        "hire_date",
        F.max("salary").over(w).alias("dept_id_max_salary"),
        F.lead("salary", 1).over(w2).alias("next_salary"),
    )
    .orderBy("level")
)
