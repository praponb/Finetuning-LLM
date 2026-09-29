from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc_nulls_last(), "id")

base = (
    df_departments
    .filter(~F.col("location").isin(["Singapore", "London", "Chiang Mai"]))
    .withColumn("dept_name_location", F.concat("dept_name", F.lit(" "), "location"))
)

result = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)
