from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "email",
        F.substring("full_name", 1, 3).alias("full_name_prefix"),
        F.concat("level", F.lit(" "), "full_name").alias("level_full_name"),
    )
)
