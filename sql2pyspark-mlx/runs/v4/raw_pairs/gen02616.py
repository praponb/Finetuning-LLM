from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary")
w2 = Window.partitionBy("job_title", "manager_id").orderBy("salary", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("dept_id").orderBy("days_since_hire_date", "id")
w4 = Window.orderBy(F.col("salary").desc(), "id")
w5 = Window.partitionBy("dept_id", "manager_id").orderBy(F.col("salary_avg_2_prior").desc_nulls_last(), "id")
w6 = Window.partitionBy("dept_id").orderBy("salary_avg_2_prior", "id")
w7 = Window.orderBy("row_num", "id").rowsBetween(-5, -1)
w8 = Window.partitionBy("level").orderBy("next_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_employees
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

with_dims = (
    ranked
    .alias("e")
    .join(df_departments.alias("p"), F.col("e.dept_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.location"))
)

base = (
    with_dims
    .filter(F.col("salary").between(69000, 76000))
    .withColumn("location_title", F.initcap(F.trim("location")))
    .withColumn("hire_date_label", F.date_format("hire_date", "yyyy-MM"))
    .withColumn("location_filled", F.coalesce("location", F.lit("NONE")))
)

cleaned = (
    base
    .filter(F.col("manager_id").isNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "level",
        "row_num",
        "location",
        "location_title",
        "hire_date_label",
        F.substring("full_name", 1, 4).alias("full_name_prefix"),
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
        F.coalesce("location", F.lit("N/A")).alias("resolved_location"),
    )
)

enriched = (
    cleaned
    .withColumn("salary_avg_2_prior", F.avg("salary").over(w2))
    .withColumn("next_salary", F.lead("salary", 1).over(w3))
    .withColumn("salary_bucket", F.ntile(3).over(w4))
)

ranked_employees = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked_employees
    .withColumn("next_salary_2", F.lead("salary", 1, 0).over(w6))
    .withColumn("salary_bucket_avg_5_prior", F.avg("salary_bucket").over(w7))
    .withColumn("first_known_location", F.first("location", ignorenulls=True).over(w8))
)
