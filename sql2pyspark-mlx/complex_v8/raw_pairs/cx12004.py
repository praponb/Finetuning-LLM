from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_enrolled_at = Window.partitionBy("course_id").orderBy("enrolled_at", "id")
w_all_by_enrolled_at = Window.orderBy(F.col("enrolled_at").desc(), "id")

base_enrollments = (
    df_enrollments
    .filter(F.col("term").isin("2025T2", "2024T1", "2024T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        F.substring(F.trim(F.col("term")), 1, 3).alias("term_prefix"),
    )
)

windowed = (
    base_enrollments
    .withColumn("enrolled_at_quintile", F.ntile(5).over(w_course_id_by_enrolled_at))
    .withColumn("enrolled_at_quartile", F.ntile(4).over(w_all_by_enrolled_at))
)

result = (
    windowed
    .select("id", "enrolled_at_quintile", "enrolled_at_quartile")
    .unpivot(
        ids=["id"],
        values=["enrolled_at_quintile", "enrolled_at_quartile"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "field")
)
