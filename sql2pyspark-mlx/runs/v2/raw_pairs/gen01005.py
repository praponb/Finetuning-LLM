from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0))) / F.count("*")), 1).alias("pct_finance"),
    )
    .filter(F.col("pct_finance") < 60)
    .select("dept_name", "pct_finance")
    .orderBy("dept_name")
    .limit(10)
)
