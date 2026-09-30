from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("location") != "Berlin"))
    .select(
        "*",
        F.coalesce("location", F.lit("Unknown")).alias("location_filled"),
        F.coalesce("dept_name", F.lit("NONE")).alias("dept_name_clean"),
    )
)

result = (
    cleaned
    .select("id")
    .subtract(
        df_departments
        .filter(F.col("budget") > 3136000)
        .select("id")
    )
)
