from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%Pim%") & F.col("manager_id").isNotNull())
    .select(
        "id",
        "email",
        "level",
        "dept_id",
        F.substring("level", 1, 4).alias("level_prefix"),
    )
    .orderBy("level")
)
