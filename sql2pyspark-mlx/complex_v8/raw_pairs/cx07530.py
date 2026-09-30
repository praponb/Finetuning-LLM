from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter((F.col("cohort") == "C2024") & (F.col("enrolled_on") >= F.lit("2024-05-18").cast("date")))
    .select(F.col("id").alias("qty"), F.col("enrolled_on").alias("event_date"))
)

student_rows_2 = (
    df_students
    .filter((~F.col("is_international")) & (F.col("cohort").isin("C2022", "C2024")))
    .select("id", "enrolled_on")
)

stacked = student_rows.union(student_rows_2)

result = (
    stacked
    .groupBy("qty")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("qty")
)
