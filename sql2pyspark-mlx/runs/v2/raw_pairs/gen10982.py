from pyspark.sql import functions as F

result = (
    df_employees
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
    )
    .orderBy("pct_l3")
)
