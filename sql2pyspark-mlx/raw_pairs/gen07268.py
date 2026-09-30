from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("full_name_length", "id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-5, -1)
w_ordered = Window.partitionBy("job_title", "level").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy(F.month("hire_date")).orderBy("full_name_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.month("hire_date"), "dept_id").orderBy("full_name_length", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy(F.year("hire_date"), "manager_id").orderBy(F.col("full_name_length").desc())

filtered = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "level",
        F.substring("level", 1, 2).alias("level_prefix"),
        F.length("full_name").alias("full_name_length"),
    )
)

filtered_employees = (
    filtered
    .filter(F.col("job_title").isNotNull() & (~F.col("level").eqNullSafe(F.lit("L5"))))
    .withColumn("full_name_level", F.concat("full_name", F.lit(" - "), "level"))
)

deduped = (
    filtered_employees
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    deduped
    .filter(F.length("full_name") > 6)
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "full_name_level",
        "row_num",
        F.avg("full_name_length").over(w2).alias("full_name_length_avg_5_prior"),
        F.avg("full_name_length").over(w_ordered).alias("full_name_length_avg_3"),
        F.last("job_title", ignorenulls=True).over(w4).alias("last_known_job_title"),
        F.avg("full_name_length").over(w5).alias("full_name_length_avg_6_prior"),
        F.dense_rank().over(w6).alias("full_name_length_dense_rank"),
    )
)

result = (
    scored
    .groupBy("dept_id")
    .agg(
        F.percentile_approx("full_name_length_avg_3", 0.5).alias("full_name_length_avg_3_p50"),
        F.percentile_approx("full_name_length_dense_rank", 0.95).alias("full_name_length_dense_rank_p95"),
        F.count("*").alias("event_count"),
        F.countDistinct("dept_id").alias("distinct_dept_ids"),
    )
    .orderBy("full_name_length_avg_3_p50")
)
