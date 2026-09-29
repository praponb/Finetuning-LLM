from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "location",
        "dept_name",
        F.when(F.col("dept_name") == "Legal", 1).otherwise(0).alias("is_legal"),
        F.concat_ws(",", "dept_name", "location").alias("dept_name_location_key"),
    )
)
