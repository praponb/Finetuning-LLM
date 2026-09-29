from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.month("hire_date").isin([4, 8, 10]) & ~F.col("is_remote") & (F.col("salary") <= 81000))
)

summary = (
    base
    .groupBy("manager_id")
    .agg(
        F.countDistinct("salary").alias("unique_managers"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)

result = (
    summary
    .filter(F.col("unique_managers") < 4)
    .orderBy("manager_id")
)
