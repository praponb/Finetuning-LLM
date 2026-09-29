from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_employees
    .filter(F.col("hire_date") > "2024-05-01")
    .select(
        "id",
        "level",
        "salary",
        "full_name",
        F.last("full_name").over(w).alias("last_full_name"),
    )
    .orderBy("full_name")
)
