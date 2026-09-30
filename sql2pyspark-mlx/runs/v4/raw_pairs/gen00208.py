from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .groupBy("is_remote")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.min("hire_date").alias("first_hire_date"),
    )
    .orderBy(F.col("first_hire_date").desc())
)
