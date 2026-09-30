from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "location",
        F.coalesce("dept_name", F.lit("Unknown")).alias("dept_name_filled"),
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
    )
)
