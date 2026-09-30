from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

with_windows = (
    df_departments
    .withColumn("last3_min_budget", F.min("budget").over(w))
    .withColumn("last3_count_budget", F.count("budget").over(w))
    .withColumn("last3_avg_budget", F.avg("budget").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("budget") >= 3305000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
        F.when(F.col("last3_count_budget") >= 13, "LARGE").otherwise("SMALL").alias("last3_count_budget_tier"),
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.round(F.sum("last3_avg_budget") / F.nullif(F.count(F.when(F.col("location") == "Tokyo", 1)), F.lit(0)), 2).alias("last3_avg_budget_per_tokyo"),
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_rows"),
        F.count("*").alias("cnt"),
    )
)
