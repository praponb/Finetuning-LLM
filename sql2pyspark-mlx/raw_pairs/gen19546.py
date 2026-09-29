from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level", "dept_id")
    .pivot("job_title", ["Analyst", "Engineer", "Designer"])
    .agg(F.avg("salary").alias("total"))
    .select(
        "level",
        "dept_id",
        F.col("Analyst").alias("analyst"),
        F.col("Engineer").alias("engineer"),
        F.col("Designer").alias("designer"),
    )
)
