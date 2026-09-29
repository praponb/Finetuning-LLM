from pyspark.sql import functions as F

src = (
    df_employees
    .filter(F.col("is_remote"))
    .withColumn("hire_date_period", F.date_trunc("month", "hire_date"))
)

employees_stats = (
    src
    .groupBy(
        F.year("hire_date").alias("hire_year"),
        F.date_format("hire_date", "yyyy-MM").alias("hire_month"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
    )
)

result = (
    employees_stats
    .filter(F.col("l2_count") > 8)
    .select("hire_year", "hire_month", "level_values", "l2_count")
    .orderBy(F.col("level_values").desc(), "hire_year", "hire_month")
    .limit(1)
)
