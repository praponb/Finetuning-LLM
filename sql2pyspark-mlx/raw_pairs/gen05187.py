from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") != "Berlin")
    .select(
        "id",
        "budget",
        F.initcap(F.trim("location")).alias("location_title"),
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
    )
)
