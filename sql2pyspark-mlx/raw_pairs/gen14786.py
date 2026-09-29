from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["HR", "Sales", "Finance"]))
    .select(
        "id",
        "location",
        F.substring("location", 1, 4).alias("location_prefix"),
        F.substring("location", 1, 2).alias("location_prefix_2"),
    )
    .orderBy(F.col("location_prefix_2").desc(), "id")
    .limit(10)
)
