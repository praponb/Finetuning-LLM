from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter(((F.col("dept_name") == "Marketing") & (F.col("budget") > 1448000)) | F.col("dept_name").isin(["Finance", "Legal"]))
)

budget_bucket_summary = (
    recent_departments
    .groupBy(
        F.when(F.col("budget") >= 4441000, "high").when(F.col("budget") >= 1175000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
)

result = budget_bucket_summary.select("budget_bucket", "location_values")
