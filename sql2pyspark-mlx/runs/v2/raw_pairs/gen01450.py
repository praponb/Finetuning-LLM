from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["New York", "Chiang Mai"]) & F.col("budget").between(2069000, 3095000))
    .groupBy(
        F.when(F.col("budget") >= 3230000, "high").when(F.col("budget") >= 946000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
        "dept_name",
    )
    .agg(
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .filter(F.col("tokyo_count") >= 5)
)
