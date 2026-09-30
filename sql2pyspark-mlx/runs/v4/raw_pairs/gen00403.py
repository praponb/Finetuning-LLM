from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4414000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.countDistinct("location").alias("distinct_locations"),
        F.count_if(F.col("dept_name") == "Finance").alias("finance_events"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
    )
    .orderBy(F.col("budget_tier").asc_nulls_first())
)
