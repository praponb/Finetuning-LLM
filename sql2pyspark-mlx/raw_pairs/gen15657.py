from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("dept_id").orderBy("hire_date", "id")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "hire_date",
        "full_name",
        "dept_id",
        "salary",
        F.last("full_name").over(w).alias("last_full_name"),
        F.lead("salary", 1).over(w2).alias("next_salary"),
    )
)
