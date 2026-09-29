from pyspark.sql import functions as F

src = (
    df_departments
    .filter(~F.col("location").isin(["Berlin", "London"]) & (F.col("location") != "Singapore") & ~F.col("location").isin(["Chiang Mai", "New York", "Singapore"]))
)

agg = (
    src
    .groupBy(
        F.when(F.col("dept_name").isin(["Engineering", "Finance"]), "core").otherwise("other").alias("dept_name_group"),
    )
    .agg(F.sum("budget").alias("total_budget"))
)

result = (
    agg
    .filter(F.col("total_budget") >= 8217000)
    .orderBy(F.col("total_budget").desc())
)
