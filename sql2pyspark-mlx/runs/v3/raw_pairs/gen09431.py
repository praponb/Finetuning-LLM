from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")

result = (
    df_employees
    .filter(F.col("manager_id") == 23)
    .select(
        "id",
        "job_title",
        "hire_date",
        "email",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 2)
    .select("id", "job_title", "hire_date", "email")
)
