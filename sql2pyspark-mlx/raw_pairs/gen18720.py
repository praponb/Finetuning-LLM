from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy(F.year("hire_date"), "level").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "salary",
        "hire_date",
        "level",
        F.row_number().over(w).alias("row_num"),
        F.lead("salary", 1).over(w2).alias("next_salary"),
    )
    .orderBy("level")
)
