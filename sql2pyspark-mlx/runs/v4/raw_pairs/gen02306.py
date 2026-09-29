from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level", "is_remote")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)
