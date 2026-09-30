from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.col("level") == "L1")
    .select(
        "id",
        "job_title",
        "salary",
        "full_name",
        F.first("full_name").over(w).alias("first_full_name"),
    )
    .orderBy(F.col("id").desc())
)
