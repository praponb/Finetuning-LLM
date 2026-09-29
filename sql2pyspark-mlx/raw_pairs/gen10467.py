from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .filter(F.col("full_name").like("P%"))
    .select(
        "id",
        "full_name",
        "email",
        "hire_date",
        F.max("salary").over(w).alias("running_max_salary"),
        F.count("salary").over(w).alias("running_count_salary"),
        F.min("salary").over(w).alias("running_min_salary"),
    )
)
