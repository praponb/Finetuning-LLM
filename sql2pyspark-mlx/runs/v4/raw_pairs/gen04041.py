from pyspark.sql import functions as F

result = (
    df_departments
    .rollup(
        F.when(F.col("budget") >= 3193000, "HIGH").otherwise("LOW").alias("budget_tier"),
        "location",
    )
    .agg(
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_count"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.count_if(F.col("location") == "New York").alias("new_york_events"),
    )
    .orderBy(F.col("new_york_events").desc_nulls_first())
)
