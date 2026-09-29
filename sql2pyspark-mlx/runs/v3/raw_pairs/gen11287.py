from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") < 72000) & (F.col("manager_id") == 19) & (F.col("level") == "L5"))
    .select(
        "hire_date",
        F.coalesce("full_name", F.lit("Unknown")).alias("full_name_filled"),
    )
)
