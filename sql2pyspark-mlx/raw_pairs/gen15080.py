from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc())

latest = (
    df_departments
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "dept_name", "location", "budget")
)

result = (
    latest
    .groupBy("dept_name")
    .agg(
        F.percentile_approx("budget", 0.95).alias("budget_p95"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Marketing", 1)), F.lit(0)), 2).alias("budget_per_marketing"),
        F.count("*").alias("cnt"),
    )
    .orderBy("cnt")
)
