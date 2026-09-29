from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "manager_id",
        "level",
        F.initcap(F.trim("full_name")).alias("full_name_title"),
        F.upper("level").alias("level_upper"),
    )
    .orderBy("full_name_title")
)
