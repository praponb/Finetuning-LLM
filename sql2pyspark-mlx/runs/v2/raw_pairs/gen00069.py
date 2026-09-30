from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNull())
    .select("id", "budget", F.round("budget", 0).alias("budget_rounded"))
    .orderBy(F.col("budget").desc(), "id")
    .limit(5)
)
