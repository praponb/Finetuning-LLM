from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "HR"]))
    .withColumn("dept_name_group", F.when(F.col("dept_name").isin(["Engineering", "Finance"]), "Group A").otherwise("Other"))
)

agg = (
    valid_departments
    .groupBy("dept_name")
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.max("budget").alias("max_budget"),
    )
)

result = (
    agg
    .select("dept_name", "location_list")
    .orderBy(F.col("dept_name").desc())
    .limit(1)
)
