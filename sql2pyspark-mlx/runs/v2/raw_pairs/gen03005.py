from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id")

result = (
    df_employees
    .filter(F.col("full_name").like("%MILLER%"))
    .select("id", "level", "salary", F.row_number().over(w).alias("row_num"))
)
