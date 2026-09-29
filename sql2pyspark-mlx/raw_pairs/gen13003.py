from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L3", "L4", "L1"]))
    .groupBy(
        F.when(F.col("level").isin(["L1", "L2"]), "core").otherwise("other").alias("level_group"),
        "level",
    )
    .agg(F.sum("salary").alias("total_salary"))
    .orderBy("level")
)
