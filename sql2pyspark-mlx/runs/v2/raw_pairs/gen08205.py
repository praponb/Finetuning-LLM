from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("dept_name").isin(["Marketing", "Finance"]))

agg = (
    recent_departments
    .groupBy(
        F.when(F.col("budget") >= 3091000, "high").when(F.col("budget") >= 1954000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"))
)

result = agg.select("budget_bucket", "dept_name_list")
