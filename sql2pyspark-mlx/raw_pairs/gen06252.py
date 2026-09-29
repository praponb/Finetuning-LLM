from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("dept_name_location", F.concat("dept_name", F.lit(" - "), "location"))
    .withColumn("location_filled", F.coalesce("location", F.lit("UNKNOWN")))
)

result = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
