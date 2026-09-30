from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc(), "id")
w3 = Window.partitionBy("location")
w4 = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_departments
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("seq_no_2", F.row_number().over(w2))
    .withColumn("location_max_budget", F.max("budget").over(w3))
    .withColumn("budget_avg_3", F.avg("budget").over(w4))
    .withColumn("last_known_dept_name", F.last("dept_name", ignorenulls=True).over(w5))
)

result = (
    enriched
    .groupBy(
        "seq_no",
        F.when(F.col("location_max_budget") >= 3393791, "HIGH").otherwise("LOW").alias("location_max_budget_tier"),
    )
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.sum(F.when(F.col("dept_name") == "Finance", F.col("budget_avg_3")).otherwise(0)).alias("finance_budget_avg_3"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("finance_budget_avg_3") > 0)
)
