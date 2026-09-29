from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~(((F.col("location") == "Singapore") & F.col("location").isin(["Tokyo", "Singapore"])) | (F.col("location") == "Bangkok")))
    .groupBy("location")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
        F.count("budget").alias("budget_count"),
    )
    .orderBy(F.col("location").desc())
    .limit(10)
)
