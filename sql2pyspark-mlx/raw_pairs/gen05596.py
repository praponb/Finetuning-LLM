from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("is_l3").desc_nulls_last(), "id")
w2 = Window.partitionBy("dept_id").orderBy("salary", "id")
w3 = Window.orderBy("salary")
w4 = Window.orderBy("is_l3", "id")
w5 = Window.orderBy("salary", "id")
w6 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("salary_rank").desc(), "id")

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-07-27"))) & F.col("dept_id").isin([2, 5, 30, 33]))
    .select(
        "*",
        F.when(F.col("level") == "L3", 1).otherwise(0).alias("is_l3"),
        F.coalesce("job_title", F.lit("OTHER")).alias("resolved_job_title"),
    )
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "salary", "is_remote", "is_l3", "resolved_job_title")
)

metrics = (
    ranked
    .select(
        "id",
        "salary",
        "resolved_job_title",
        F.rank().over(w3).alias("salary_rank"),
        F.lag("salary", 2).over(w4).alias("prev_salary"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "resolved_job_title",
        F.lead("prev_salary", 2).over(w5).alias("next_prev_salary"),
        F.sum("salary_rank").over(w6).alias("running_salary_rank"),
        F.row_number().over(w5).alias("event_seq"),
        F.row_number().over(w7).alias("seq_no"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("event_seq") >= 7, "HIGH").otherwise("LOW").alias("event_seq_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.avg("running_salary_rank").alias("avg_running_salary_rank"),
    )
    .filter(F.col("avg_event_seq") > 0)
)
