from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc(), "id")

valid = (
    df_departments
    .filter(F.col("budget") <= 3102000)
    .withColumn("clean_location", F.trim("location"))
    .withColumn("dept_name_clean", F.coalesce("dept_name", F.lit("OTHER")))
)

result = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
