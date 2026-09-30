from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")

valid = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "London"]))
    .select(
        "*",
        F.when(F.col("dept_name") == "HR", 1).otherwise(0).alias("is_hr"),
        F.length("location").alias("location_length"),
        F.coalesce("location", F.lit("UNKNOWN")).alias("resolved_location"),
    )
)

result = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "dept_name", "location", "budget", "is_hr", "location_length")
)
