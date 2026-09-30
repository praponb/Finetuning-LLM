from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())
w2 = Window.partitionBy("dept_name").orderBy("budget", "id")
w_running = Window.orderBy(F.col("budget").desc(), "id")
w_part = Window.partitionBy("location").orderBy(F.col("dedup_rank").desc(), "id")
w5 = Window.partitionBy("dept_name", "location").orderBy(F.col("row_seq").desc())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.substring("dept_name", 1, 4).alias("dept_name_prefix"),
        F.coalesce("location", F.lit("DIRECT")).alias("resolved_location"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .filter(F.col("dedup_rank") < 1)
    .select(
        "*",
        F.row_number().over(w2).alias("row_seq"),
        F.ntile(5).over(w_running).alias("budget_bucket"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

latest = (
    enriched
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select("id", "dept_name", "resolved_location")
)

latest_match = (
    latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name", F.col("c.salary"))
)

summary = (
    latest_match
    .groupBy(
        F.when(F.col("salary") >= 121000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.sum(F.when(F.col("dept_name") == "Finance", F.col("salary")).otherwise(0)).alias("finance_salary"),
        F.percentile_approx("salary", 0.95).alias("salary_p95"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.count_if(F.col("dept_name") == "Engineering").alias("engineering_events"),
    )
)

result = (
    summary
    .select(
        "salary_tier",
        "finance_salary",
        "num_rows",
        "num_dept_names",
        "engineering_events",
    )
    .orderBy(F.col("salary_tier").desc_nulls_first())
)
