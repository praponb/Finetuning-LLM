from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location")
w_recent = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("dept_name")
w5 = Window.partitionBy("dept_name").orderBy("budget", "id")

filtered = (
    df_departments
    .filter(F.col("location") != "Singapore")
    .withColumn("dept_name_location_key", F.concat_ws("-", "dept_name", "location"))
    .withColumn("resolved_location", F.coalesce("location", F.lit("DIRECT")))
)

result = (
    filtered
    .select(
        "*",
        F.sum("budget").over(w_grp).alias("running_budget"),
        F.min("budget").over(w2).alias("location_min_budget"),
        F.avg("budget").over(w_recent).alias("budget_avg_2_prior"),
        F.round((F.col("budget") / F.sum("budget").over(w4)), 4).alias("budget_share"),
        F.lead("budget", 2).over(w5).alias("next_budget"),
    )
)
