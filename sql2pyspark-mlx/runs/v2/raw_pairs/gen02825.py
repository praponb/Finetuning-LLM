from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "manager_id")
w2 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .filter(F.col("email").isNull())
    .select(
        "id",
        "level",
        "manager_id",
        "salary",
        "dept_id",
        "hire_date",
        F.round((F.col("salary") / F.sum("salary").over(w)), 3).alias("salary_share"),
        F.sum("salary").over(w2).alias("running_salary"),
    )
    .orderBy("running_salary", "salary")
)
