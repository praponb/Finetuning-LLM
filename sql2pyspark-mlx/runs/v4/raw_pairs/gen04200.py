from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")

ranked = (
    df_departments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("budget") >= 3637000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("avg_budget").desc_nulls_first())
)
