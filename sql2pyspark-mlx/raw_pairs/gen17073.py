from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget_whole").desc())

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (~F.col("location").eqNullSafe(F.lit("Tokyo"))))
    .select(
        "*",
        F.col("budget").cast("int").alias("budget_int"),
        F.floor("budget").alias("budget_whole"),
        F.coalesce("location", F.lit("DIRECT")).alias("location_clean"),
    )
)

result = (
    base
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)
