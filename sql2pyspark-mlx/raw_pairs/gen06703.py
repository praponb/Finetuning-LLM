from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.date_format("hire_date", "MMM yyyy").alias("hire_date_label"),
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)

result = (
    valid
    .groupBy("level")
    .agg(
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_count"),
        F.round(F.avg("is_remote_flag"), 2).alias("avg_is_remote_flag"),
    )
)
