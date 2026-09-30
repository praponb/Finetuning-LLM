from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "London") & (F.col("location") != "Singapore"))
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3992000, "high").when(F.col("budget") >= 995000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max("budget").alias("max_budget"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
    .filter(F.col("cnt") < 2)
)
