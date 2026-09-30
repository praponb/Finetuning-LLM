from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "id",
        "job_title",
        "salary",
        "full_name",
        F.last("full_name").over(w).alias("last_full_name"),
    )
)
