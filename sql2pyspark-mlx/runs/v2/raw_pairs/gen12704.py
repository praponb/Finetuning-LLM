from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "job_title")

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "job_title",
        F.count("*").over(w).alias("manager_id_count"),
    )
    .orderBy("job_title")
)
