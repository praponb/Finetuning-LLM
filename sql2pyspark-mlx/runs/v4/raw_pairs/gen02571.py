from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

ranked = (
    df_departments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy("dept_name")
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.sum("budget").alias("total_budget"),
        F.countDistinct("location").alias("distinct_locations"),
    )
    .filter(F.col("avg_budget") > 0)
    .orderBy("distinct_locations")
)
