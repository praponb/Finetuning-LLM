from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "location",
        F.concat("dept_name", F.lit("/"), "location").alias("dept_name_location"),
        F.when(F.col("location") == "Chiang Mai", 1).otherwise(0).alias("is_chiang_mai"),
    )
)
