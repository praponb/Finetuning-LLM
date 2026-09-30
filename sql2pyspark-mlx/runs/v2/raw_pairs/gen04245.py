from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L4", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l4"),
    )
)
