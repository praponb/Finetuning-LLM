from pyspark.sql import functions as F

base = (
    df_departments
    .filter((F.col("budget") < 2906000) & F.col("location").isin(["Tokyo", "Singapore"]))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
)

agg = (
    base
    .groupBy("location", "dept_name")
    .agg(F.count(F.when(F.col("location") == "New York", 1)).alias("new_york_count"))
)

result = agg
