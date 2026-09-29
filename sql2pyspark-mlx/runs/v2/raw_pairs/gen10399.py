from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("salary") < 130000)
    .select(
        "id",
        "job_title",
        "hire_date",
        F.row_number().over(w).alias("hire_date_row"),
    )
)
