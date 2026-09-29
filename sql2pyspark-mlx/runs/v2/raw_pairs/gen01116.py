from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") != "Engineering")
    .select(
        "id",
        "dept_name",
        "location",
        F.concat_ws("/", "dept_name", "location").alias("dept_name_location_key"),
        F.when(F.col("location").isin(["London", "Chiang Mai"]), "Group A").otherwise("Other").alias("location_group"),
    )
)
