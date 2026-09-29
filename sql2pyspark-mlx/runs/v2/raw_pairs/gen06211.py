from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "Berlin"]) | F.col("dept_name").isin(["Sales", "Marketing"]))
    .select(
        "id",
        "dept_name",
        "location",
        F.trim("location").alias("clean_location"),
        F.when(F.col("location") == "Tokyo", 1).otherwise(0).alias("is_tokyo"),
    )
    .orderBy(F.col("id").desc(), "location")
)
