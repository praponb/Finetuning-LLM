from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("level").isin(["L2", "L3"]), "priority").otherwise("other").alias("level_group"),
    )
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.max("hire_date").alias("last_hire_date"),
    )
    .orderBy("last_hire_date")
)
