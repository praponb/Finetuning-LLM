from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("location").orderBy("budget", "id")

metrics = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("next_budget", F.lead("budget").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_grp))
)

result = (
    metrics
    .groupBy("location")
    .agg(
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_rows"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_count"),
    )
    .orderBy(F.col("bangkok_count").asc_nulls_first())
)
