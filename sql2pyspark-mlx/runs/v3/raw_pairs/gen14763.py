from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc())

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "hire_date",
        "manager_id",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 2)
)
