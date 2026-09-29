from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("level", "job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("hire_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level")
w5 = Window.orderBy("hire_date", "id").rowsBetween(-2, -1)

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-04-17"))) & F.col("level").isNotNull() & F.col("email").like("%@gmail.com"))
    .select(
        "id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.add_months("hire_date", 3).alias("hire_date_plus_months"),
        F.coalesce("job_title", F.lit("N/A")).alias("resolved_job_title"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.last("job_title", ignorenulls=True).over(w2).alias("last_known_job_title"),
        F.first("job_title", ignorenulls=True).over(w3).alias("first_known_job_title"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.count("row_seq").over(w4).alias("level_count_row_seq"),
        F.avg("row_seq").over(w5).alias("row_seq_avg_2_prior"),
    )
)

result = (
    scored
    .withColumn("level_count_row_seq_tier", F.when(F.col("level_count_row_seq") >= 15, "HIGH").otherwise("LOW"))
    .cube("level_count_row_seq_tier")
    .agg(
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_rows"),
        F.size(F.collect_set("level")).alias("num_levels_2"),
        F.sum(F.when(F.col("level") == "L5", F.col("row_seq_avg_2_prior")).otherwise(0)).alias("l5_row_seq_avg_2_prior"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("num_levels_2").desc())
)
