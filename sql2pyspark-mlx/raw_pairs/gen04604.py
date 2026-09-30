from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")

base = (
    df_departments
    .filter(~F.col("location").eqNullSafe(F.lit("Berlin")))
    .select(
        "*",
        F.trim("location").alias("clean_location"),
        F.coalesce("location", F.lit("N/A")).alias("location_clean"),
    )
)

result = (
    base
    .select(
        "*",
        F.avg("budget").over(w).alias("budget_avg_6"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
