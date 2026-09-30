from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") < 133000) | F.col("manager_id").isNull())
    .groupBy("is_remote")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0))) / F.count("*")), 1).alias("pct_director"),
    )
)
