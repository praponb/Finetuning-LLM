from pyspark.sql import functions as F

course_side = (
    df_courses
    .filter((F.length(F.trim(F.col("course_code"))) >= 5) & (F.col("dept").isin("Bio", "Art", "Math")))
    .select(
        F.col("dept").alias("label"),
        F.col("id").alias("ref_id"),
        F.col("level").alias("category"),
    )
)

course_side_2 = (
    df_courses
    .filter(F.length(F.trim(F.col("course_code"))) >= 6)
    .select(
        F.col("course_code").alias("label"),
        F.col("credits").alias("ref_id"),
        F.col("dept").alias("category"),
    )
)

combined = (
    course_side
    .unionByName(course_side_2)
    .distinct()
)

result = (
    combined
    .groupBy("label")
    .agg(F.count("*").alias("n_rows"), F.sum("ref_id").alias("total_ref_id"))
    .orderBy("label")
)
