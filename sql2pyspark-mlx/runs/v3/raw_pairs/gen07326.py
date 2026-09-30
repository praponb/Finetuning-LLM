from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter(F.col("location").isin(["New York", "Bangkok", "London"]))
    .withColumn("is_london", F.when(F.col("location") == "London", 1).otherwise(0))
)

summary = (
    recent_departments
    .groupBy(
        F.when(F.col("dept_name").isin(["Finance", "HR"]), "group_a").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
    )
)

result = (
    summary
    .select("dept_name_group", "dept_name_values")
    .orderBy("dept_name_group")
)
