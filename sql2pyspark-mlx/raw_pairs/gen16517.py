from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 1711000)
    .select(
        "id",
        "location",
        "dept_name",
        F.when(F.col("dept_name") == "Legal", 1).otherwise(0).alias("is_legal"),
    )
    .orderBy(F.col("id").desc(), F.col("location").desc())
)
