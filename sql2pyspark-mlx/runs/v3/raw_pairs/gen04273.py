from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") <= 3210000) & (F.col("location") == "Berlin"))
    .select("budget", F.coalesce("location", F.lit("Unknown")).alias("location_filled"))
    .orderBy(F.col("budget").desc())
)
