from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2046000)
    .select(
        "location",
        F.upper("location").alias("location_upper"),
        F.when(F.col("dept_name").isin(["Engineering", "Legal"]), "Tier 1").otherwise("Other").alias("dept_name_group"),
    )
    .orderBy("dept_name_group")
)
