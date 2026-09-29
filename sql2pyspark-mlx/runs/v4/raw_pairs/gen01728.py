from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 1591000) & F.col("budget").between(1604000, 1867000) & (F.col("location") == "Chiang Mai"))
    .select(
        "id",
        "location",
        F.when(F.col("dept_name") == "Finance", 1).otherwise(0).alias("is_finance"),
    )
    .orderBy(F.col("is_finance").desc(), "id")
    .limit(10)
)
