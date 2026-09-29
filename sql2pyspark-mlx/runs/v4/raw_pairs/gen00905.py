from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy("location_length", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_first())
w4 = Window.orderBy("budget", "id")

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("location_length", F.length("location"))
    .withColumn("location_filled", F.coalesce("location", F.lit("OTHER")))
)

kept = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.job_title") == "Analyst"), "left_semi")
)

enriched = (
    kept
    .select(
        "*",
        F.lag("location_length", 2).over(w).alias("prev_location_length"),
        F.avg("location_length").over(w2).alias("location_length_avg_3"),
        F.rank().over(w3).alias("budget_rank"),
        F.ntile(4).over(w4).alias("budget_bucket"),
    )
)

result = (
    enriched
    .select(
        "id",
        "budget",
        "location_length",
        "location_filled",
        "location_length_avg_3",
        "budget_bucket",
    )
    .orderBy("budget_bucket", "location_length")
)
