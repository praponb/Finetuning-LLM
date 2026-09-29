from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull() & F.col("dept_name").isin(["HR", "Marketing", "Legal"]))
    .select("budget", "location", F.coalesce("budget", F.lit(0)).alias("budget_filled"))
)
