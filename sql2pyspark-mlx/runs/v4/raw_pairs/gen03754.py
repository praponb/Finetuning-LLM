from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id")
w2 = Window.partitionBy("level", "manager_id").orderBy(F.col("salary").desc(), "id")

metrics = (
    df_employees
    .withColumn("salary_bucket", F.ntile(4).over(w))
    .withColumn("salary_bucket_2", F.ntile(3).over(w2))
)

result = (
    metrics
    .withColumn("salary_tier", F.when(F.col("salary") >= 163000, "LARGE").otherwise("SMALL"))
    .withColumn("salary_bucket_2_tier", F.when(F.col("salary_bucket_2") >= 7, "PREMIUM").otherwise("STANDARD"))
    .cube("salary_tier", "salary_bucket_2_tier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("level") == "L3", F.col("salary_bucket_2")).otherwise(0)).alias("l3_salary_bucket_2"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.count("*").alias("row_count"),
        F.min("salary_bucket").alias("min_salary_bucket"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("row_count").desc())
)
