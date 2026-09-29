from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id") == 25)
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"), "level")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L1", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l1"),
    )
    .filter(F.col("pct_l1") >= 49)
    .orderBy("pct_l1")
)
