from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Sales") & F.col("dept_name").isin(["Sales", "Finance"]) & F.col("location").isNotNull())
    .select(
        "id",
        "budget",
        F.concat_ws("/", "dept_name", "location").alias("dept_name_location_key"),
        F.when(F.col("location").isin(["New York", "Berlin"]), "Core").otherwise("Other").alias("location_group"),
    )
    .orderBy(F.col("location_group").desc())
)
