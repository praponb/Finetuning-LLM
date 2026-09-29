from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([3, 17, 25, 37]))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L1", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l1"),
    )
    .orderBy(F.col("pct_l1").desc())
)
