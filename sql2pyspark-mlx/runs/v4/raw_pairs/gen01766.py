from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("budget").desc_nulls_last())

metrics = (
    df_departments
    .withColumn("first_known_location", F.first("location", ignorenulls=True).over(w))
    .withColumn("budget_cume_dist", F.cume_dist().over(w2))
)

result = (
    metrics
    .withColumn("budget_cume_dist_tier", F.when(F.col("budget_cume_dist") >= 0.89, "HIGH").otherwise("LOW"))
    .cube("dept_name", "budget_cume_dist_tier")
    .agg(
        F.percentile_approx("budget_cume_dist", [0.25, 0.5, 0.75]).alias("budget_cume_dist_quartiles"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.round(F.avg("budget_cume_dist"), 2).alias("avg_budget_cume_dist"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("budget_cume_dist", 0.95).alias("budget_cume_dist_p95"),
        F.grouping("dept_name").alias("grouping_id"),
    )
    .orderBy("budget_cume_dist_tier")
)
