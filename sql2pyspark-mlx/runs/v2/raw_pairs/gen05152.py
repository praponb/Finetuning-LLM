from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "hire_date",
        "salary",
        F.last("salary").over(w).alias("last_salary"),
    )
)
