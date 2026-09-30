from pyspark.sql import functions as F

result = (
    df_employees
    .rollup(
        "is_remote",
        F.when(F.col("level").isin(["L4", "L2"]), "tier_1").otherwise("other").alias("level_group"),
    )
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)
