from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc())

enriched = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        F.min("budget").over(w_ordered).alias("running_min_budget"),
        F.sum("budget").over(w_ordered).alias("running_sum_budget"),
        F.max("budget").over(w_ordered).alias("running_max_budget"),
        F.cume_dist().over(w_part).alias("budget_cume_dist"),
    )
)

result = (
    enriched
    .select(
        "id",
        "dept_name",
        "location",
        F.when(F.col("location").rlike("^(Singapore|Berlin)$"), "GROUP_A").when(F.col("location").rlike("^(Bangkok|Chiang Mai)$"), "PRIORITY").otherwise("CORE").alias("location_zone"),
    )
    .orderBy(F.col("location").asc_nulls_last(), F.col("dept_name").desc())
)
