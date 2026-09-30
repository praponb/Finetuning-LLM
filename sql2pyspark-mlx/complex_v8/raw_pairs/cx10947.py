from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("major").orderBy(F.col("enrolled_on").desc(), F.col("id").desc())

base = (
    df_students
    .filter(F.col("major") == "Biz")
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .groupBy("enrolled_on", "is_international")
    .pivot("major", ["CS", "Bio", "Biz"])
    .agg(F.sum("gpa"))
)
