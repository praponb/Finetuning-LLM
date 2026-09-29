from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "Berlin"]) & ((F.col("dept_name") != "Legal") | (F.col("location") != "Bangkok")))
    .groupBy(
        "dept_name",
        "location",
        F.when(F.col("budget") >= 3579000, "high").when(F.col("budget") >= 1761000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sort_array(F.collect_set("location")).alias("location_list"))
    .orderBy(F.col("budget_bucket").desc(), "location")
)
