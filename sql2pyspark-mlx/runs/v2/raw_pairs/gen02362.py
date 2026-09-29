from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Finance", "Marketing"]) & (F.col("dept_name") == "Marketing"))
    .groupBy("dept_name")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_legal"),
        F.round(((100 * F.sum(F.when(F.col("location") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
    )
    .orderBy("pct_legal")
)
