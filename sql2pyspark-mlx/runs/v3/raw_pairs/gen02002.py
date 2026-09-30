from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") != "Marketing") & (F.col("dept_name") == "Legal"))
    .select(
        "id",
        "budget",
        F.length("location").alias("location_length"),
        F.round("budget", 0).alias("budget_rounded"),
    )
    .orderBy(F.col("location_length").desc())
)
