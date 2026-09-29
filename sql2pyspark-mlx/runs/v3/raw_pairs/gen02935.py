from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Finance") | (F.col("budget") < 2719000))
    .select(
        "id",
        "location",
        "budget",
        F.when(F.col("location") == "Chiang Mai", 1).otherwise(0).alias("is_chiang_mai"),
    )
    .orderBy("is_chiang_mai", "id")
    .limit(50)
)
