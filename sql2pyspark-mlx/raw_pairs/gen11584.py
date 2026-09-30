from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("email").like("%@company.co") | (F.col("salary") > 82000))
    .groupBy(
        F.when(F.col("level").isin(["L5", "L4"]), "tier_1").otherwise("other").alias("level_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L4", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l4"),
        F.sum("salary").alias("total_salary"),
    )
    .orderBy(F.col("pct_l4").desc())
)
