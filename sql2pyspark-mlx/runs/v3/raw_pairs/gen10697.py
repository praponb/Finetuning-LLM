from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Engineering", "Finance"]))
    .select("id", "location", "budget", F.row_number().over(w).alias("rn"))
)
