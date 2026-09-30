from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "New York", "Chiang Mai"]))
    .select("*", F.coalesce("budget", F.lit(0)).alias("budget_filled"))
)

result = (
    valid
    .groupBy(
        F.when(F.col("budget_filled") >= 86, "PREMIUM").otherwise("STANDARD").alias("budget_filled_tier"),
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.round(F.avg("budget_filled"), 2).alias("avg_budget_filled"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("location") == "Bangkok", 1)), F.lit(0)), 2).alias("budget_per_bangkok"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("budget_per_bangkok") > 0)
)
