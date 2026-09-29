from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2858000)
    .select(
        "location",
        "budget",
        F.when(F.col("location").isin(["New York", "Chiang Mai"]), "Tier 1").otherwise("Other").alias("location_group"),
        F.initcap(F.trim("location")).alias("location_title"),
    )
    .orderBy(F.col("budget").desc())
)
