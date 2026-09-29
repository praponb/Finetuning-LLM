from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("cohort").orderBy("enrolled_on", "id")

filtered = (
    df_students
    .filter(F.col("cohort").isin("C2022", "C2024"))
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "is_international",
        F.abs(F.col("gpa") - 3.2).alias("gpa_distance"),
        F.concat(F.col("student_name"), F.lit("#"), F.col("id").cast("string")).alias("student_name_ref"),
    )
)

unique_rows = (
    filtered
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .groupBy("enrolled_on")
    .pivot("cohort", ["C2023", "C2024"])
    .agg(F.count("gpa_distance").alias("count_v"), F.sum("gpa_distance").alias("sum_v"))
    .orderBy("enrolled_on")
)
