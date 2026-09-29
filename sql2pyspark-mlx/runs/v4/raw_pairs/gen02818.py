from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("dept_name_location", F.concat("dept_name", F.lit(" - "), "location"))
    .withColumn("location_clean", F.coalesce("location", F.lit("DIRECT")))
)

result = (
    filtered
    .select(
        "*",
        F.sum("budget").over(w).alias("cumulative_budget"),
        F.first("location", ignorenulls=True).over(w2).alias("first_known_location"),
    )
)
