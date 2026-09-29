from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([4, 22]))
    .select(
        "manager_id",
        "full_name",
        F.substring("full_name", 1, 3).alias("full_name_prefix"),
    )
    .orderBy("full_name", "full_name_prefix")
)
