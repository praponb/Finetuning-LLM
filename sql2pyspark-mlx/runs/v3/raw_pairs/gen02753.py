from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level", "manager_id")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
    )
)
