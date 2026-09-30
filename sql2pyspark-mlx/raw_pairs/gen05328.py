from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_last(), "id")

latest = (
    df_departments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.when(F.col("dept_name").rlike("^(HR|Legal)$"), "GROUP_B").otherwise("PRIORITY").alias("dept_name_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first())
    .limit(10)
)
