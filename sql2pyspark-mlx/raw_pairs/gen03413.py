from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "*",
        F.lead("budget").over(w).alias("next_budget"),
        F.row_number().over(w).alias("seq_no"),
    )
)
