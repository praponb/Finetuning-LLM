from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_departments
    .filter(F.col("location") == "Chiang Mai")
    .select(
        "*",
        F.when(F.col("budget") >= 4305000, "High").when(F.col("budget") >= 1931000, "Medium").otherwise("Low").alias("budget_band"),
        F.substring("location", 1, 2).alias("location_prefix"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("dept_name_clean"),
    )
)

result = (
    valid
    .select(
        "id",
        "location_prefix",
        "dept_name_clean",
        F.ntile(4).over(w).alias("budget_bucket"),
        F.sum("budget").over(w2).alias("cumulative_budget"),
    )
)
