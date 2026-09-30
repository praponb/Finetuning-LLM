from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("major", "cohort").orderBy(F.col("enrolled_on").desc(), F.col("id").desc())

filtered = df_students.filter((F.col("cohort") == "C2023") & (F.col("major").isin("Biz", "Bio")))

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

major_stats = (
    deduped
    .groupBy("major")
    .agg(F.max("gpa").alias("max_gpa"))
)

above_average = (
    deduped.alias("s")
    .join(major_stats.alias("g"), F.col("g.major") == F.col("s.major"), "inner")
    .filter(F.col("s.gpa") == F.col("g.max_gpa"))
    .select(
        "s.*",
        "g.max_gpa",
        F.round(F.col("s.gpa") - F.col("g.max_gpa"), 2).alias("gpa_minus_max"),
    )
)

result = (
    above_average
    .filter(F.col("gpa").between(2.1, 3.4))
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        "max_gpa",
        "gpa_minus_max",
        F.length(F.trim(F.col("major"))).alias("major_len"),
        F.coalesce(F.col("gpa"), F.lit(0.0)).alias("gpa_filled"),
    )
    .orderBy(
        F.col("id").desc_nulls_first(),
        F.col("gpa_minus_max").desc_nulls_first(),
    )
)
