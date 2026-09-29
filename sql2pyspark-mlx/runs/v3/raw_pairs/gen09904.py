from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([13, 22, 24, 38]) & F.col("full_name").like("Ka%") & (F.lower(F.trim("full_name")) == "david lee"))
    .groupBy("level", "manager_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L5", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l5"),
    )
    .orderBy("level")
)
