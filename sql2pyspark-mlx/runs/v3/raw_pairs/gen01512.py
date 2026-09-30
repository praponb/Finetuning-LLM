from pyspark.sql import functions as F

base = df_employees.filter(~F.col("is_remote") & F.col("level").isin(["L2", "L5", "L4"]))

level_summary = (
    base
    .groupBy("level")
    .agg(F.sort_array(F.collect_set("level")).alias("level_list"))
)

result = level_summary.select("level", "level_list")
