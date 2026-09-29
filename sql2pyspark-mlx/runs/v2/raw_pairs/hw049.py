from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = (
    Window.partitionBy("dept_id")
    .orderBy("salary")
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)

result = df_employees.select("id", "dept_id", "salary", F.last("salary").over(w).alias("highest_salary"))
