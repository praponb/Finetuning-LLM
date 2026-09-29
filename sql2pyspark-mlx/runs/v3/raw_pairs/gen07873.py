from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 1)
)
