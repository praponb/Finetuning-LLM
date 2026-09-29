from pyspark.sql import functions as F

src = df_employees.filter(F.col("manager_id").isin([9, 18, 28, 33]))

employees_stats = (
    src
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)

result = employees_stats.orderBy(F.col("hire_quarter").desc())
