from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Tokyo") | (F.col("budget") > 1993000))
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3967000, "high").when(F.col("budget") >= 1833000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
)
