from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc_nulls_last())
w2 = Window.partitionBy(F.month("hire_date")).orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("job_title", "manager_id")
w4 = Window.partitionBy("manager_id").orderBy("rank_in_group", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("level", "location").orderBy("salary_avg_3", "id")
w6 = Window.partitionBy("manager_id").orderBy(F.col("salary_share").desc(), "id")

ranked = (
    df_employees
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

joined = (
    ranked
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.budget") > 1264000)
    .select("e.*", F.col("d.budget"), F.col("d.location"), F.col("d.dept_name"))
)

with_windows = (
    joined
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "is_remote",
        "level",
        "rank_in_group",
        "location",
        "dept_name",
        F.ntile(5).over(w2).alias("salary_bucket"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
        F.avg("salary").over(w4).alias("salary_avg_3"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "is_remote",
        "level",
        "location",
        "salary_bucket",
        "salary_avg_3",
        F.lag("salary_bucket").over(w5).alias("prev_salary_bucket"),
        F.lead("salary_share").over(w5).alias("next_salary_share"),
        F.row_number().over(w5).alias("row_seq"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("next_salary_share") >= 0.81, "PREMIUM").otherwise("STANDARD").alias("next_salary_share_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.round(F.sum("salary_avg_3") / F.nullif(F.count(F.when(F.col("location") == "London", 1)), F.lit(0)), 2).alias("salary_avg_3_per_london"),
        F.percentile_approx("salary_bucket", [0.25, 0.5, 0.75]).alias("salary_bucket_quartiles"),
    )
    .orderBy("salary_avg_3_per_london")
)
