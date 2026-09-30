from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%FRANK%") & (F.col("salary") >= 86000))
    .select(
        "id",
        "manager_id",
        F.concat("level", F.lit(", "), "full_name").alias("level_full_name"),
        F.round("salary", 2).alias("salary_rounded"),
    )
)
