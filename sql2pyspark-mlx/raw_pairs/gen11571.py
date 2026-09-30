from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") > 144000) & F.col("is_remote"))
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"), "job_title")
    .agg(
        F.sum(F.when(F.col("level") == "L1", 1).otherwise(0)).alias("l1_count"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.sum(F.when(F.col("level") == "L5", 1).otherwise(0)).alias("l5_count"),
    )
    .filter(F.col("l5_count") >= 56)
    .drop("l5_count")
    .orderBy(F.col("l1_count").desc(), "job_title")
)
