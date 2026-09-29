from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter((F.col("dept_name") == "Finance") & F.col("budget").between(2195000, 2916000))
    .withColumn("location_group", F.when(F.col("location").isin(["London", "Chiang Mai"]), "Group A").otherwise("Other"))
)

agg = (
    filtered_departments
    .groupBy("dept_name")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.min("budget").alias("min_budget"),
    )
)

result = agg.filter(F.col("min_budget") > 2415000)
