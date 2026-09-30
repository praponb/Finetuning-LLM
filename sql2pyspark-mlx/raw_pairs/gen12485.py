from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")
w2 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_id").orderBy("budget", "id").rowsBetween(-5, Window.currentRow)
w_recent = Window.partitionBy("department_location").orderBy(F.col("budget").desc(), "id")
w5 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("budget").asc_nulls_last())
w7 = Window.orderBy("first_known_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("department_location")
w9 = Window.partitionBy("job_title").orderBy("budget_bucket_share", "id")
w10 = Window.orderBy(F.col("budget_bucket_share").desc_nulls_last())
w11 = Window.partitionBy("location")

deduped = (
    df_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "full_name", "email", "dept_id", "job_title", "salary", "hire_date")
)

joined = (
    deduped
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.location"), F.col("b.dept_name"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "inner")
    .select(
        "src.*",
        F.col("p.location").alias("department_location"),
        F.col("p.budget"),
        F.col("p.dept_name").alias("department_dept_name"),
    )
)

metrics = (
    with_dims
    .filter(F.length("email") > 7)
    .withColumn("first_known_budget", F.first("budget", ignorenulls=True).over(w2))
    .withColumn("salary_avg_5", F.avg("salary").over(w3))
    .withColumn("budget_bucket", F.ntile(5).over(w_recent))
    .withColumn("cumulative_budget", F.sum("budget").over(w5))
    .withColumn("budget_dense_rank", F.dense_rank().over(w6))
)

enriched = (
    metrics
    .select(
        "id",
        "email",
        "job_title",
        "location",
        "dept_name",
        "department_location",
        "department_dept_name",
        "first_known_budget",
        F.first("dept_name", ignorenulls=True).over(w7).alias("first_known_dept_name"),
        F.round((F.col("budget_bucket") / F.sum("budget_bucket").over(w8)), 4).alias("budget_bucket_share"),
    )
)

enriched_employees = (
    enriched
    .select(
        "*",
        F.lag("first_known_budget").over(w9).alias("prev_first_known_budget"),
        F.lead("budget_bucket_share").over(w9).alias("next_budget_bucket_share"),
        F.lag("budget_bucket_share").over(w9).alias("prev_budget_bucket_share"),
        F.row_number().over(w9).alias("event_seq"),
        F.first("first_known_budget", ignorenulls=True).over(w7).alias("first_known_first_known_budget"),
    )
)

scored = (
    enriched_employees
    .select(
        "id",
        "job_title",
        "department_dept_name",
        "budget_bucket_share",
        "prev_first_known_budget",
        "next_budget_bucket_share",
        "prev_budget_bucket_share",
        "event_seq",
        F.dense_rank().over(w10).alias("budget_bucket_share_dense_rank"),
        F.round((F.col("first_known_first_known_budget") / F.sum("first_known_first_known_budget").over(w11)), 4).alias("first_known_first_known_budget_share"),
    )
)

summary = (
    scored
    .withColumn("event_seq_tier", F.when(F.col("event_seq") >= 9, "HIGH").otherwise("LOW"))
    .withColumn("prev_budget_bucket_share_tier", F.when(F.col("prev_budget_bucket_share") >= 0.66, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["event_seq_tier", "prev_budget_bucket_share_tier"], ["event_seq_tier"], []],
        "event_seq_tier",
        "prev_budget_bucket_share_tier",
    )
    .agg(
        F.sum(F.when(F.col("department_dept_name") == "Sales", F.col("budget_bucket_share_dense_rank")).otherwise(0)).alias("sales_budget_bucket_share_dense_rank"),
        F.countDistinct("job_title").alias("distinct_job_titles"),
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_rows"),
        F.count("*").alias("cnt"),
    )
)

result = (
    summary
    .select(
        "prev_budget_bucket_share_tier",
        "sales_budget_bucket_share_dense_rank",
        "director_rows",
    )
    .orderBy(F.col("prev_budget_bucket_share_tier").desc_nulls_last())
)
