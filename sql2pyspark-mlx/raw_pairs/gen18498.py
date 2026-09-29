from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 1807000) & (F.col("dept_name") != "Marketing"))
    .groupBy(
        F.when(F.col("budget") >= 3222000, "high").when(F.col("budget") >= 1801000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(F.sort_array(F.collect_set("location")).alias("location_list"))
    .orderBy("dept_name")
)
