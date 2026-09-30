from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "location",
        "dept_name",
        F.when(F.col("dept_name").isin(["Marketing", "Finance"]), "Core").otherwise("Other").alias("dept_name_group"),
    )
    .orderBy("dept_name_group", "id")
    .limit(1)
)
