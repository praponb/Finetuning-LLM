from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3650000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
)
