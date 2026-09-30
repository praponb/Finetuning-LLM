from pyspark.sql import functions as F

enrollment_rows = df_enrollments.select("id", F.col("score").alias("metric"))

enrollment_side = (
    df_enrollments
    .filter(F.col("score").between(5.9, 78.8))
    .select("student_id", "score")
)

all_events = enrollment_rows.union(enrollment_side)

result = (
    all_events
    .groupBy("id")
    .agg(F.count("*").alias("n_rows"), F.sum("metric").alias("total_metric"))
    .orderBy("id")
)
