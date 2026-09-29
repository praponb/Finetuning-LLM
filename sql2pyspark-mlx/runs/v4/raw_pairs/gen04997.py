from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L2", "L3"]) & ((F.length("email") >= 14) | (F.col("salary") > 134000)))
    .groupBy(
        F.month("hire_date").alias("hire_month"),
        F.when(F.col("level").isin(["L1", "L4"]), "core").otherwise("other").alias("level_group"),
        F.year("hire_date").alias("hire_year"),
    )
    .agg(F.sum(F.when(F.col("level") == "L1", 1).otherwise(0)).alias("l1_count"))
)
